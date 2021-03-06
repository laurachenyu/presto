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
package io.prestosql.plugin.hive.rubix;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.bookkeeper.BookKeeperServer;
import com.qubole.rubix.bookkeeper.LocalDataTransferServer;
import com.qubole.rubix.core.CachingFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoAzureBlobFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoDistributedFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoGoogleHadoopFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoNativeAzureFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoS3FileSystem;
import com.qubole.rubix.prestosql.CachingPrestoSecureAzureBlobFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoSecureNativeAzureFileSystem;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.ConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.util.RetryDriver;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.qubole.rubix.spi.CacheConfig.enableHeartbeat;
import static com.qubole.rubix.spi.CacheConfig.setBookKeeperServerPort;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataDirPrefix;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataEnabled;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataExpirationAfterWrite;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataFullnessPercentage;
import static com.qubole.rubix.spi.CacheConfig.setClusterNodeRefreshTime;
import static com.qubole.rubix.spi.CacheConfig.setCoordinatorHostName;
import static com.qubole.rubix.spi.CacheConfig.setDataTransferServerPort;
import static com.qubole.rubix.spi.CacheConfig.setEmbeddedMode;
import static com.qubole.rubix.spi.CacheConfig.setIsParallelWarmupEnabled;
import static com.qubole.rubix.spi.CacheConfig.setOnMaster;
import static com.qubole.rubix.spi.CacheConfig.setPrestoClusterManager;
import static io.prestosql.plugin.hive.DynamicConfigurationProvider.setCacheKey;
import static io.prestosql.plugin.hive.util.ConfigurationUtils.getInitialConfiguration;
import static io.prestosql.plugin.hive.util.RetryDriver.DEFAULT_SCALE_FACTOR;
import static io.prestosql.plugin.hive.util.RetryDriver.retry;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Integer.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/*
 * Responsibilities of this initializer:
 * 1. Wait for master and setup RubixConfigurationInitializer with information about master when it becomes available
 * 2. Start Rubix Servers.
 * 3. Inject BookKeeper object into CachingFileSystem class
 * 4. Update HDFS configuration
 */
public class RubixInitializer
{
    private static final String RUBIX_S3_FS_CLASS_NAME = CachingPrestoS3FileSystem.class.getName();

    private static final String RUBIX_NATIVE_AZURE_FS_CLASS_NAME = CachingPrestoNativeAzureFileSystem.class.getName();
    private static final String RUBIX_SECURE_NATIVE_AZURE_FS_CLASS_NAME = CachingPrestoSecureNativeAzureFileSystem.class.getName();
    private static final String RUBIX_AZURE_BLOB_FS_CLASS_NAME = CachingPrestoAzureBlobFileSystem.class.getName();
    private static final String RUBIX_SECURE_AZURE_BLOB_FS_CLASS_NAME = CachingPrestoSecureAzureBlobFileSystem.class.getName();

    private static final String RUBIX_GS_FS_CLASS_NAME = CachingPrestoGoogleHadoopFileSystem.class.getName();

    private static final String RUBIX_DISTRIBUTED_FS_CLASS_NAME = CachingPrestoDistributedFileSystem.class.getName();

    private static final RetryDriver DEFAULT_COORDINATOR_RETRY_DRIVER = retry()
            // unlimited attempts
            .maxAttempts(MAX_VALUE)
            .exponentialBackoff(
                    new Duration(1, SECONDS),
                    new Duration(1, SECONDS),
                    // wait for 10 minutes
                    new Duration(10, MINUTES),
                    DEFAULT_SCALE_FACTOR);

    private static final Logger log = Logger.get(RubixInitializer.class);

    private final RetryDriver coordinatorRetryDriver;
    private final boolean startServerOnCoordinator;
    private final boolean parallelWarmupEnabled;
    private final String cacheLocation;
    private final long cacheTtlMillis;
    private final int diskUsagePercentage;
    private final int bookKeeperServerPort;
    private final int dataTransferServerPort;
    private final NodeManager nodeManager;
    private final CatalogName catalogName;
    private final HdfsConfigurationInitializer hdfsConfigurationInitializer;
    private final Optional<ConfigurationInitializer> extraConfigInitializer;

    private volatile boolean cacheReady;
    @Nullable
    private HostAddress masterAddress;
    @Nullable
    private BookKeeperServer bookKeeperServer;

    @Inject
    public RubixInitializer(
            RubixConfig rubixConfig,
            NodeManager nodeManager,
            CatalogName catalogName,
            HdfsConfigurationInitializer hdfsConfigurationInitializer,
            @ForRubix Optional<ConfigurationInitializer> extraConfigInitializer)
    {
        this(DEFAULT_COORDINATOR_RETRY_DRIVER, rubixConfig, nodeManager, catalogName, hdfsConfigurationInitializer, extraConfigInitializer);
    }

    @VisibleForTesting
    RubixInitializer(
            RetryDriver coordinatorRetryDriver,
            RubixConfig rubixConfig,
            NodeManager nodeManager,
            CatalogName catalogName,
            HdfsConfigurationInitializer hdfsConfigurationInitializer,
            Optional<ConfigurationInitializer> extraConfigInitializer)
    {
        this.coordinatorRetryDriver = coordinatorRetryDriver;
        this.startServerOnCoordinator = rubixConfig.isStartServerOnCoordinator();
        this.parallelWarmupEnabled = rubixConfig.getReadMode().isParallelWarmupEnabled();
        this.cacheLocation = rubixConfig.getCacheLocation();
        this.cacheTtlMillis = rubixConfig.getCacheTtl().toMillis();
        this.diskUsagePercentage = rubixConfig.getDiskUsagePercentage();
        this.bookKeeperServerPort = rubixConfig.getBookKeeperServerPort();
        this.dataTransferServerPort = rubixConfig.getDataTransferServerPort();
        this.nodeManager = nodeManager;
        this.catalogName = catalogName;
        this.hdfsConfigurationInitializer = hdfsConfigurationInitializer;
        this.extraConfigInitializer = extraConfigInitializer;
    }

    void initializeRubix()
    {
        if (nodeManager.getCurrentNode().isCoordinator() && !startServerOnCoordinator) {
            // setup JMX metrics on master (instead of starting server) so that JMX connector can be used
            // TODO: remove once https://github.com/prestosql/presto/issues/3821 is fixed
            setupRubixMetrics();
            return;
        }

        waitForCoordinator();
        startRubix();
    }

    @PreDestroy
    public void stopRubix()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> {
                if (bookKeeperServer != null) {
                    // This might throw NPE if Thrift server hasn't started yet (it's initialized
                    // asynchronously from BookKeeperServer thread).
                    // TODO: improve stopping of BookKeeperServer server in Rubix
                    bookKeeperServer.stopServer();
                    bookKeeperServer = null;
                }
            });
            closer.register(LocalDataTransferServer::stopServer);
        }
    }

    public void enableRubix(Configuration configuration)
    {
        if (!cacheReady) {
            disableRubix(configuration);
            return;
        }

        updateRubixConfiguration(configuration);
        setCacheKey(configuration, "rubix_enabled");
    }

    public void disableRubix(Configuration configuration)
    {
        setCacheDataEnabled(configuration, false);
        setCacheKey(configuration, "rubix_disabled");
    }

    private void waitForCoordinator()
    {
        try {
            coordinatorRetryDriver.run(
                    "waitForCoordinator",
                    () -> {
                        if (nodeManager.getAllNodes().stream().noneMatch(Node::isCoordinator)) {
                            // This exception will only be propagated when timeout is reached.
                            throw new PrestoException(GENERIC_INTERNAL_ERROR, "No coordinator node available");
                        }
                        return null;
                    });
        }
        catch (Exception exception) {
            propagateIfPossible(exception, PrestoException.class);
            throw new RuntimeException(exception);
        }
    }

    private void startRubix()
    {
        Configuration configuration = getRubixServerConfiguration();

        MetricRegistry metricRegistry = new MetricRegistry();
        bookKeeperServer = new BookKeeperServer();
        BookKeeper bookKeeper = bookKeeperServer.startServer(configuration, metricRegistry);
        LocalDataTransferServer.startServer(configuration, metricRegistry, bookKeeper);

        CachingFileSystem.setLocalBookKeeper(bookKeeper, "catalog=" + catalogName);
        log.info("Rubix initialized successfully");
        cacheReady = true;
    }

    private void setupRubixMetrics()
    {
        Configuration configuration = getRubixServerConfiguration();
        new BookKeeperServer().setupServer(configuration, new MetricRegistry());
        CachingFileSystem.setLocalBookKeeper(null, "catalog=" + catalogName);
    }

    private Configuration getRubixServerConfiguration()
    {
        Node master = nodeManager.getAllNodes().stream().filter(Node::isCoordinator).findFirst().get();
        masterAddress = master.getHostAndPort();

        Configuration configuration = getInitialConfiguration();
        // Perform standard HDFS configuration initialization.
        hdfsConfigurationInitializer.initializeConfiguration(configuration);
        updateRubixConfiguration(configuration);
        setCacheKey(configuration, "rubix_internal");

        return configuration;
    }

    void updateRubixConfiguration(Configuration config)
    {
        checkState(masterAddress != null, "masterAddress is not set");
        setCacheDataEnabled(config, true);
        setOnMaster(config, nodeManager.getCurrentNode().isCoordinator());
        setCoordinatorHostName(config, masterAddress.getHostText());

        setIsParallelWarmupEnabled(config, parallelWarmupEnabled);
        setCacheDataDirPrefix(config, cacheLocation);
        setCacheDataExpirationAfterWrite(config, cacheTtlMillis);
        setCacheDataFullnessPercentage(config, diskUsagePercentage);
        setBookKeeperServerPort(config, bookKeeperServerPort);
        setDataTransferServerPort(config, dataTransferServerPort);

        setEmbeddedMode(config, true);
        enableHeartbeat(config, false);
        setClusterNodeRefreshTime(config, 10);

        config.set("fs.s3.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3a.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3n.impl", RUBIX_S3_FS_CLASS_NAME);

        config.set("fs.wasb.impl", RUBIX_NATIVE_AZURE_FS_CLASS_NAME);
        config.set("fs.wasbs.impl", RUBIX_SECURE_NATIVE_AZURE_FS_CLASS_NAME);
        config.set("fs.abfs.impl", RUBIX_AZURE_BLOB_FS_CLASS_NAME);
        config.set("fs.abfss.impl", RUBIX_SECURE_AZURE_BLOB_FS_CLASS_NAME);

        config.set("fs.gs.impl", RUBIX_GS_FS_CLASS_NAME);

        config.set("fs.hdfs.impl", RUBIX_DISTRIBUTED_FS_CLASS_NAME);

        // TODO: fix PrestoClusterManager in Rubix itself
        PrestoClusterManager.setNodeManager(nodeManager);
        setPrestoClusterManager(config, PrestoClusterManager.class.getName());

        extraConfigInitializer.ifPresent(initializer -> initializer.initializeConfiguration(config));
    }
}
