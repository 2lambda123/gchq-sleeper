/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.compaction.job.execution;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.execution.testutils.CompactSortedFilesTestBase;
import sleeper.compaction.job.execution.testutils.CompactSortedFilesTestData;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static sleeper.compaction.job.execution.testutils.CompactSortedFilesTestUtils.assignJobIdToInputFiles;
import static sleeper.compaction.job.execution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class CompactSortedFilesLocalStackIT extends CompactSortedFilesTestBase {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(
            DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(S3, DYNAMODB);
    private static AmazonDynamoDB dynamoDBClient;
    private static AmazonS3 s3Client;
    private static S3AsyncClient s3AsyncClient;

    @BeforeAll
    public static void beforeAll() {
        dynamoDBClient = buildAwsV1Client(localStackContainer, DYNAMODB, AmazonDynamoDBClientBuilder.standard());
        s3Client = buildAwsV1Client(localStackContainer, S3, AmazonS3ClientBuilder.standard());
        s3AsyncClient = buildAwsV2Client(localStackContainer, S3, S3AsyncClient.builder());
    }

    @AfterAll
    public static void afterAll() {
        dynamoDBClient.shutdown();
        s3Client.shutdown();
        s3AsyncClient.close();
    }

    @BeforeEach
    void setUp() {
        instanceProperties.resetAndValidate(createTestInstanceProperties().getProperties());
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
        new S3StateStoreCreator(instanceProperties, dynamoDBClient).create();
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    protected FileReference ingestRecordsGetFile(StateStore stateStore, List<Record> records) throws Exception {
        return ingestRecordsGetFile(records, builder -> builder
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .hadoopConfiguration(getHadoopConfiguration(localStackContainer))
                .s3AsyncClient(s3AsyncClient));
    }

    private StateStore createStateStore(Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0");
        return new StateStoreFactory(dynamoDBClient, s3Client, instanceProperties, getHadoopConfiguration(localStackContainer))
                .getStateStore(tableProperties);
    }

    private CompactSortedFiles createCompactSortedFiles(Schema schema, StateStore stateStore) throws Exception {
        tableProperties.setSchema(schema);
        return new CompactSortedFiles(instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                ObjectFactory.noUserJars());
    }

    @Test
    public void shouldUpdateStateStoreAfterRunningCompactionJob() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        StateStore stateStore = createStateStore(schema);
        PartitionTree tree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        stateStore.initialise(tree.getAllPartitions());

        List<Record> data1 = CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs();
        FileReference file1 = ingestRecordsGetFile(stateStore, data1);
        FileReference file2 = ingestRecordsGetFile(stateStore, data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, stateStore);
        RecordsProcessed summary = compactSortedFiles.compact(compactionJob);

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = CompactSortedFilesTestData.combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        Assertions.assertThat(CompactSortedFilesTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check StateStore has correct ready for GC files
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactlyInAnyOrder(file1.getFilename(), file2.getFilename());

        // - Check StateStore has correct file references
        assertThat(stateStore.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(FileReferenceFactory.from(tree).rootFile(compactionJob.getOutputFile(), 200L));
    }
}
