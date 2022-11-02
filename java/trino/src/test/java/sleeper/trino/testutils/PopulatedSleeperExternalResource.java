/*
 * Copyright 2022 Crown Copyright
 *
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
package sleeper.trino.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.google.common.collect.ImmutableList;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.StandardIngestCoordinator;
import sleeper.statestore.InitialiseStateStore;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.trino.SleeperConfig;
import sleeper.trino.remotesleeperconnection.HadoopConfigurationProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TABLE_PROPERTIES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * This class is a JUnit {@link ExternalResource} which starts a local S3 and DynamoDB within a Docker
 * LocalStackContainer. Sleeper tables are created, with the data files themselves stored in a temporary directory on
 * the local disk.
 * <p>
 * Only use one instance of this class at once. The Parquet readers and writers make use of the Hadoop FileSystem and
 * this caches the S3AFileSystem objects which actually communicate with S3. The cache needs to be reset between
 * different recreations of the localstack container. It would be good to fix this in future.
 */
public class PopulatedSleeperExternalResource extends ExternalResource {
    private static final String TEST_CONFIG_BUCKET_NAME = "test-config-bucket";

    private final Map<String, String> extraPropertiesForQueryRunner;
    private final List<TableDefinition> tableDefinitions;
    private final SleeperConfig sleeperConfig;
    private final TemporaryFolder temporaryFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private final LocalStackContainer localStackContainer =
            new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
                    .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3)
                    .withLogConsumer(outputFrame -> System.out.print("LocalStack log: " + outputFrame.getUtf8String()))
                    .withEnv("DEBUG", "1");
    private final HadoopConfigurationProvider hadoopConfigurationProvider = new HadoopConfigurationProviderForLocalStack(localStackContainer);

    private BufferAllocator rootBufferAllocator;
    private AmazonS3 s3Client;
    private S3AsyncClient s3AsyncClient;
    private AmazonDynamoDB dynamoDBClient;
    private QueryAssertions queryAssertions;

    public PopulatedSleeperExternalResource(Map<String, String> extraPropertiesForQueryRunner,
                                            List<TableDefinition> tableDefinitions,
                                            Optional<SleeperConfig> sleeperConfigOpt) {
        this.extraPropertiesForQueryRunner = requireNonNull(extraPropertiesForQueryRunner);
        this.tableDefinitions = requireNonNull(tableDefinitions);
        this.sleeperConfig = requireNonNull(sleeperConfigOpt).orElse(new SleeperConfig());
    }

    private AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .build();
    }

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .build();
    }

    private S3AsyncClient createS3AsyncClient() {
        return S3AsyncClient.builder()
                .endpointOverride(localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey()
                )))
                .region(Region.of(localStackContainer.getRegion()))
                .build();
    }

    private void ingestData(InstanceProperties instanceProperties,
                            StateStore stateStore,
                            TableProperties tableProperties,
                            Iterator<Record> recordIterator)
            throws Exception {
        Configuration hadoopConfiguration = this.hadoopConfigurationProvider.getHadoopConfiguration(instanceProperties);
        IngestCoordinator<Record> ingestRecordsAsync = StandardIngestCoordinator.asyncS3WriteBackedByArrow(
                new ObjectFactory(instanceProperties, null, temporaryFolder.toString()),
                stateStore,
                tableProperties.getSchema(),
                temporaryFolder.newFolder().getAbsolutePath(),
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "snappy",
                hadoopConfiguration,
                null,
                null,
                120,
                tableProperties.get(DATA_BUCKET),
                this.s3AsyncClient,
                this.rootBufferAllocator,
                30,
                16 * 1024 * 1024L,
                16 * 1024 * 1024L,
                32 * 1024 * 1024L,
                200L);
        while (recordIterator.hasNext()) {
            ingestRecordsAsync.write(recordIterator.next());
        }
        ingestRecordsAsync.close();
    }

    private InstanceProperties createInstanceProperties() throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(CONFIG_BUCKET, TEST_CONFIG_BUCKET_NAME);
        instanceProperties.set(JARS_BUCKET, "");
        instanceProperties.set(ACCOUNT, "");
        instanceProperties.set(REGION, "");
        instanceProperties.set(VERSION, "");
        instanceProperties.set(VPC_ID, "");
        instanceProperties.set(SUBNET, "");
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.set(TABLE_PROPERTIES, "");

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        instanceProperties.saveToS3(s3Client);

        return instanceProperties;
    }

    private TableProperties createTable(InstanceProperties instanceProperties,
                                        TableDefinition tableDefinition) throws IOException, StateStoreException {
        // Use the table name to generate the data bucket name, abiding by the bucket-naming rules
        String dataBucket = tableDefinition.tableName
                .toLowerCase()
                .replaceAll("[^-a-z0-9.]", "---");

        String activeTable = tableDefinition.tableName + "-af";
        String readyForGCTable = tableDefinition.tableName + "-rfgcf";
        String partitionTable = tableDefinition.tableName + "-p";
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableDefinition.tableName);
        tableProperties.setSchema(tableDefinition.schema);
        tableProperties.set(DATA_BUCKET, dataBucket);
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, activeTable);
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, readyForGCTable);
        tableProperties.set(PARTITION_TABLENAME, partitionTable);
        tableProperties.saveToS3(this.s3Client);

        s3Client.createBucket(dataBucket);

        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(
                instanceProperties,
                tableProperties,
                this.dynamoDBClient);
        dynamoDBStateStoreCreator.create();
        return tableProperties;
    }

    private InstanceProperties getInstanceProperties() throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(this.s3Client, TEST_CONFIG_BUCKET_NAME);
        return instanceProperties;
    }

    private TableProperties getTableProperties(String tableName) throws IOException {
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(this.s3Client, this.getInstanceProperties());
        return tablePropertiesProvider.getTableProperties(tableName);
    }

    public StateStore getStateStore(String tableName) throws IOException {
        StateStoreProvider stateStoreProvider = new StateStoreProvider(this.dynamoDBClient, this.getInstanceProperties());
        return stateStoreProvider.getStateStore(this.getTableProperties(tableName));
    }

    @Override
    protected void before() throws Throwable {
        this.temporaryFolder.create();
        this.localStackContainer.start();
        this.s3Client = createS3Client();
        this.s3AsyncClient = createS3AsyncClient();
        this.dynamoDBClient = createDynamoClient();
        this.rootBufferAllocator = new RootAllocator();

        System.out.println("S3 endpoint:       " + localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3).getServiceEndpoint());
        System.out.println("DynamoDB endpoint: " + localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3).getServiceEndpoint());

        InstanceProperties instanceProperties = createInstanceProperties();

        this.tableDefinitions.forEach(tableDefinition -> {
            try {
                TableProperties tableProperties = createTable(
                        instanceProperties,
                        tableDefinition);
                StateStoreProvider stateStoreProvider = new StateStoreProvider(this.dynamoDBClient, instanceProperties);
                StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
                InitialiseStateStore initialiseStateStore = new InitialiseStateStore(tableDefinition.schema, stateStore, tableDefinition.splitPoints);
                initialiseStateStore.run();
                ingestData(instanceProperties, stateStore, tableProperties, tableDefinition.recordStream.iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        System.out.println("--- My buckets ---");
        List<Bucket> buckets = s3Client.listBuckets();
        buckets.forEach(System.out::println);
        buckets.forEach(bucket -> {
            System.out.printf("--- Contents of bucket %s ---%n", bucket);
            s3Client.listObjectsV2(bucket.getName()).getObjectSummaries().forEach(System.out::println);
        });

        this.sleeperConfig.setConfigBucket(TEST_CONFIG_BUCKET_NAME);
        DistributedQueryRunner distributedQueryRunner = SleeperQueryRunner.createSleeperQueryRunner(
                this.extraPropertiesForQueryRunner,
                this.sleeperConfig,
                this.s3Client,
                this.s3AsyncClient,
                this.dynamoDBClient,
                this.hadoopConfigurationProvider);
        this.queryAssertions = new QueryAssertions(distributedQueryRunner);
    }

    @Override
    protected void after() {
        this.queryAssertions.close();
        this.s3Client.shutdown();
        this.dynamoDBClient.shutdown();
        this.localStackContainer.stop();
        this.temporaryFolder.delete();
        this.rootBufferAllocator.close();

        // The Hadoop file system maintains a cache of the file system object to use. The S3AFileSystem object
        // retains the endpoint URL and so the cache needs to be cleared whenever the localstack instance changes.
        try {
            FileSystem.closeAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public QueryAssertions getQueryAssertions() {
        return this.queryAssertions;
    }

    public static class TableDefinition {
        public final String tableName;
        public final Schema schema;
        public final List<Object> splitPoints;
        public final Stream<Record> recordStream;

        public TableDefinition(String tableName,
                               Schema schema,
                               Optional<List<Object>> splitPointsOpt,
                               Optional<Stream<Record>> recordStreamOpt) {
            this.tableName = tableName;
            this.schema = schema;
            this.splitPoints = splitPointsOpt.orElse(ImmutableList.of());
            this.recordStream = recordStreamOpt.orElse(Stream.empty());
        }
    }
}
