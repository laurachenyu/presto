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
package io.prestosql.plugin.hive.metastore;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.coral.hive.hive2rel.HiveMetastoreClient;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import org.apache.hadoop.hive.metastore.api.Database;

import java.util.List;
import java.util.Optional;

/**
 * Class to adapt Presto's {@link SemiTransactionalHiveMetastore} to Coral's
 * {@link HiveMetastoreClient}. This allows reuse of the hive metastore instantiated by
 * Presto, based on configuration, inside Coral.
 */
public class CoralSemiTransactionalHiveMSCAdapter
        implements HiveMetastoreClient
{
    private final SemiTransactionalHiveMetastore delegate;
    private final HiveIdentity identity;

    public CoralSemiTransactionalHiveMSCAdapter(SemiTransactionalHiveMetastore msc, HiveIdentity identity)
    {
        this.delegate = msc;
        this.identity = identity;
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    // returning null for missing entry is as per Coral's requirements
    @Override
    public Database getDatabase(String dbName)
    {
        return delegate.getDatabase(dbName).map(ThriftMetastoreUtil::toMetastoreApiDatabase).orElse(null);
    }

    @Override
    public List<String> getAllTables(String dbName)
    {
        return delegate.getAllTables(dbName);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName)
    {
        Optional<io.prestosql.plugin.hive.metastore.Table> table = delegate.getTable(identity, dbName, tableName);
        // TODO: Get the right privileges here. This could be problematic if Coral needs permissions of the table.
        return table.map(value -> ThriftMetastoreUtil.toMetastoreApiTable(value, new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of())))
                .orElse(null);
    }
}
