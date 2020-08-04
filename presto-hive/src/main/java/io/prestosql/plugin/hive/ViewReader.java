/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive;

import com.google.common.base.Preconditions;
import com.linkedin.coral.hive.hive2rel.HiveMetastoreClient;
import com.linkedin.coral.hive.hive2rel.HiveToRelConverter;
import com.linkedin.coral.presto.rel2presto.RelToPrestoConverter;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.CoralSemiTransactionalHiveMSCAdapter;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hadoop.hive.metastore.TableType;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static io.prestosql.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.prestosql.plugin.hive.util.HiveUtil.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

/**
 * Decode view definitions stored in Hive metastore. This class instantiates
 * correct decoder based on the type of view (hive or presto view).
 */
public abstract class ViewReader
{
    public static final String PRESTO_VIEW_FLAG = "presto_view";

    static final String VIEW_PREFIX = "/* Presto View: ";
    static final String VIEW_SUFFIX = " */";

    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC =
            new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ConnectorViewDefinition.class);

    private static Logger log = Logger.get(ViewReader.class);

    public static boolean isPrestoView(Table table)
    {
        return "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG));
    }

    public static boolean isHiveOrPrestoView(Table table)
    {
        return table.getTableType().equals(TableType.VIRTUAL_VIEW.name());
    }

    public static boolean canDecodeView(Table table)
    {
        // we can decode Hive or Presto view
        return table.getTableType().equals(VIRTUAL_VIEW.name());
    }

    public static String encodeViewData(ConnectorViewDefinition definition)
    {
        byte[] bytes = VIEW_CODEC.toJsonBytes(definition);
        String data = Base64.getEncoder().encodeToString(bytes);
        return VIEW_PREFIX + data + VIEW_SUFFIX;
    }

    /**
     * Decodes view definition stored in hive metastore. Currently, it supports decoding
     * hive or presto view definitions.
     *
     * @param metastore hive metastore to read view definitions from. This is required
     * to decode Hive view definitions.
     * @param table virtual table (view) instance
     * @return json representation of view definition
     */
    public static ConnectorViewDefinition decodeView(SemiTransactionalHiveMetastore metastore, HiveIdentity identity, Table table, CatalogName catalogName, TypeManager typemanager)
    {
        ViewReader viewReader = create(metastore, identity, table, typemanager);
        return viewReader.decodeViewData(table.getViewOriginalText().get(), table, catalogName);
    }

    public static ViewReader create(SemiTransactionalHiveMetastore metastore, HiveIdentity identity, Table table, TypeManager typemanager)
    {
        if (isPrestoView(table)) {
            return new PrestoViewDecoder();
        }
        else {
            return new HiveViewDecoder(new CoralSemiTransactionalHiveMSCAdapter(metastore, identity), typemanager);
        }
    }

    /**
     * Decode view definition
     *
     * @param viewData view definition stored as "View Original Text" in Hive metastore
     * @param table table representing virtual view to decode
     * @return json representation of view definition
     */
    public abstract ConnectorViewDefinition decodeViewData(String viewData, Table table, CatalogName catalogName);

    /**
     * Supports decoding of Presto views
     */
    public static class PrestoViewDecoder
            extends ViewReader
    {
        @Override
        public ConnectorViewDefinition decodeViewData(String viewData, Table table, CatalogName catalogName)
        {
            checkCondition(viewData.startsWith(VIEW_PREFIX), HIVE_INVALID_VIEW_DATA, "View data missing prefix: %s", viewData);
            checkCondition(viewData.endsWith(VIEW_SUFFIX), HIVE_INVALID_VIEW_DATA, "View data missing suffix: %s", viewData);
            viewData = viewData.substring(VIEW_PREFIX.length());
            viewData = viewData.substring(0, viewData.length() - VIEW_SUFFIX.length());
            byte[] bytes = Base64.getDecoder().decode(viewData);
            return VIEW_CODEC.fromJson(bytes);
        }
    }

    /**
     * Class to decode Hive view definitions
     */
    public static class HiveViewDecoder
            extends ViewReader
    {
        private final HiveMetastoreClient mscClient;
        private final TypeManager typeManager;

        public HiveViewDecoder(HiveMetastoreClient mscClient, TypeManager typemanager)
        {
            this.mscClient = mscClient;
            this.typeManager = requireNonNull(typemanager, "metadata is null");
        }

        @Override
        public ConnectorViewDefinition decodeViewData(String viewSql, Table table, CatalogName catalogName)
        {
            try {
                HiveToRelConverter hiveToRelConverter = HiveToRelConverter.create(mscClient);
                RelNode rel = hiveToRelConverter.convertView(table.getDatabaseName(), table.getTableName());
                RelToPrestoConverter rel2Presto = new RelToPrestoConverter();
                String prestoSql = rel2Presto.convert(rel);
                RelDataType rowType = rel.getRowType();
                List<ConnectorViewDefinition.ViewColumn> columns = new ArrayList<>(rowType.getFieldCount());
                for (RelDataTypeField field : rowType.getFieldList()) {
                    log.debug("Adding column %s, %s, %s, %s", field.getName(),
                            field.getType().getFullTypeString(),
                            field.getType().getSqlTypeName(),
                            field);

                    Type type = typeManager.fromSqlType(getTypeString(field.getType()));
                    ConnectorViewDefinition.ViewColumn column = new ConnectorViewDefinition.ViewColumn(field.getName(), type.getTypeId());
                    columns.add(column);
                }
                return new ConnectorViewDefinition(prestoSql,
                        Optional.of(catalogName.toString()),
                        Optional.of(table.getDatabaseName()),
                        columns,
                        Optional.ofNullable(table.getParameters().get(TABLE_COMMENT)),
                        Optional.empty(),
                        true);
            }
            catch (RuntimeException e) {
                throw new PrestoException(HIVE_UNKNOWN_ERROR,
                        format("Error decoding view definition for %s.%s: %s. Please report this to ask_dali@linkedin.com",
                                table.getDatabaseName(),
                                table.getTableName(),
                                e.getMessage()),
                        e);
            }
        }

        // Calcite does not provide correct type strings for non-primitive types.
        // We add custom code here to make it work. Goal is for calcite/coral to handle this
        private String getTypeString(RelDataType type)
        {
            switch (type.getSqlTypeName()) {
                case ROW: {
                    Preconditions.checkState(type.isStruct());
                    StringBuilder sb = new StringBuilder("row(");
                    List<RelDataTypeField> fieldList = type.getFieldList();
                    for (int i = 0; i < fieldList.size(); i++) {
                        if (i != 0) {
                            // There should not be space after comma. Presto deserializer doesn't like it
                            sb.append(",");
                        }
                        RelDataTypeField field = fieldList.get(i);
                        sb.append(field.getName().toLowerCase(Locale.ENGLISH)).append(" ").append(getTypeString(field.getType()));
                    }
                    sb.append(")");
                    return sb.toString();
                }
                case CHAR:
                    return "varchar";
                case FLOAT:
                    return "real";
                case BINARY:
                case VARBINARY:
                    return "varbinary";
                case MAP: {
                    RelDataType keyType = type.getKeyType();
                    RelDataType valueType = type.getValueType();
                    StringBuilder sb = new StringBuilder("map(");
                    sb.append(getTypeString(keyType))
                            .append(",")
                            .append(getTypeString(valueType));
                    sb.append(")");
                    return sb.toString();
                }
                case ARRAY: {
                    StringBuilder sb = new StringBuilder("array(");
                    sb.append(getTypeString(type.getComponentType()));
                    sb.append(")");
                    return sb.toString();
                }
                case DECIMAL: {
                    StringBuilder sb = new StringBuilder("decimal(");
                    sb.append(type.getPrecision())
                            .append(",")
                            .append(type.getScale())
                            .append(")");
                    return sb.toString();
                }
                default:
                    return type.getSqlTypeName().toString();
            }
        }
    }
}
