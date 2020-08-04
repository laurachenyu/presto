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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.prestosql.Session;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.MetastoreUtil;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.plugin.hive.testing.TestingHivePlugin;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.TestingSession;
import io.prestosql.tpch.TpchTable;
import org.apache.hadoop.hive.metastore.TableType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.QueryAssertions.copyTpchTables;

public class TestCoralIntegration
        extends AbstractTestQueryFramework
{
    private static final String HIVE_CATALOG = "hive";
    private File catalogDirectory = Files.createTempDir();

    @AfterClass(alwaysRun = true)
    public void close()
    {
        try {
            deleteRecursively(catalogDirectory.toPath(), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = TestingSession.testSessionBuilder().setCatalog(HIVE_CATALOG).build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        // Create tpch catalog
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

        // Create hive catalog
        FileHiveMetastore metastore = FileHiveMetastore.createTestingFileHiveMetastore(catalogDirectory);
        queryRunner.installPlugin(new TestingHivePlugin(metastore));
        queryRunner.createCatalog(HIVE_CATALOG, "hive", ImmutableMap.of());

        // Populate TPC-H tables to Hive
        metastore.createDatabase(new HiveIdentity(session.toConnectorSession()), new Database(TPCH_SCHEMA, Optional.empty(), "ignored", PrincipalType.USER, Optional.empty(), ImmutableMap.of()));
        Session tpchSession = Session.builder(queryRunner.getDefaultSession()).setSchema("tpch").build();
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSession, ImmutableList.of(TpchTable.ORDERS));

        // And create the Hive-defined views we'll use for testing
        createHiveView(
                metastore,
                new HiveIdentity(session.toConnectorSession()),
                "tpch",
                "counting_view",
                "select count(1) as count from tpch.orders",
                "select count(1) as `count` from `tpch`.`orders`");
        createHiveView(
                metastore,
                new HiveIdentity(session.toConnectorSession()),
                "tpch",
                "zero_index_view",
                // Arrays in Hive are zero-indexed. This query will return 'hive' only if it's being parsed as a Hive query
                "select array('presto','hive')[1] as sql_dialect",
                "select array('presto','hive')[1] as `sql_dialect`");

        return queryRunner;
    }

    private static void createHiveView(FileHiveMetastore metastore, HiveIdentity identity, String databaseName, String tableName, String originalViewText, String expandedViewText)
    {
        Storage storage = Storage.builder()
                .setLocation("")
                .setStorageFormat(StorageFormat.VIEW_STORAGE_FORMAT)
                .setBucketProperty(Optional.empty())
                .setSerdeParameters(ImmutableMap.of())
                .build();
        metastore.createTable(identity, new Table(
                        databaseName,
                        tableName,
                        "admin",
                        TableType.VIRTUAL_VIEW.toString(),
                        storage,
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        Optional.of(originalViewText),
                        Optional.of(expandedViewText)),
                MetastoreUtil.buildInitialPrivilegeSet("admin"));
    }

    // The intent here is not to exhaustively test Coral itself, only that we have properly integrated with it. Queries of all different shapes are tested within Coral.
    @Test
    public void testSimpleCoralIntegration()
    {
        assertQuery("SELECT * FROM hive.tpch.counting_view", "SELECT count(1) FROM orders");
        assertQuery("SELECT * FROM hive.tpch.zero_index_view", "SELECT 'hive'");
    }
}
