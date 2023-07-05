/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.Lists;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStoreCreator;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.ingest.job.status.IngestJobStatusTestData.acceptedRun;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.rejectedRun;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

@Testcontainers
class BulkImportExecutorIT {
    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate")
            .withIgnoredFieldsMatchingRegexes("jobRun.+updateTime").build();
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private AmazonDynamoDB createDynamoDBClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private static final Schema SCHEMA = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    private final AmazonS3 s3 = createS3Client();
    private final AmazonDynamoDB dynamoDB = createDynamoDBClient();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, SCHEMA);
    private final String bucketName = UUID.randomUUID().toString();
    private final String tableName = "myTable";
    private final IngestJobStatusStore ingestJobStatusStore = new DynamoDBIngestJobStatusStore(dynamoDB, instanceProperties);

    @BeforeEach
    void setup() {
        DynamoDBIngestJobStatusStoreCreator.create(instanceProperties, dynamoDB);
        instanceProperties.set(BULK_IMPORT_BUCKET, bucketName);
        s3.createBucket(bucketName);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(SCHEMA);
        tableProperties.set(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
    }

    @AfterEach
    public void tearDown() {
        dynamoDB.deleteTable(DynamoDBIngestJobStatusStore.jobStatusTableName(instanceProperties.get(ID)));
        s3.shutdown();
    }

    @Nested
    @DisplayName("Failing validation")
    class FailValidation {
        @Test
        void shouldFailValidationIfFileListIsEmpty() {
            // Given
            BulkImportJob importJob = new BulkImportJob.Builder()
                    .tableName(tableName)
                    .id("my-job")
                    .files(Lists.newArrayList())
                    .build();
            FakeExecutor fakeExecutor = buildExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));

            // When
            fakeExecutor.runJob(importJob);

            // Then
            assertThat(fakeExecutor.getJobsRun()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableName))
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                    .containsExactly(jobStatus(importJob.toIngestJob(),
                            rejectedRun(importJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                    "The input files must be set to a non-null and non-empty value.")));
        }

        @Test
        void shouldFailValidationIfJobPointsAtNonExistentTable() {
            // Given
            BulkImportJob importJob = new BulkImportJob.Builder()
                    .tableName("table-that-does-not-exist")
                    .id("my-job")
                    .files(Lists.newArrayList("file1.parquet"))
                    .build();
            FakeExecutor fakeExecutor = buildExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));

            // When
            fakeExecutor.runJob(importJob);

            // Then
            assertThat(fakeExecutor.getJobsRun()).isEmpty();
            assertThat(ingestJobStatusStore.getJob("my-job")).isPresent().get()
                    .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                    .isEqualTo(jobStatus(importJob.toIngestJob(),
                            rejectedRun(importJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                    "Table does not exist.")));
        }

        @Test
        void shouldFailValidationIfTableNameIsNull() {
            // Given
            BulkImportJob importJob = new BulkImportJob.Builder()
                    .id("my-job")
                    .files(Lists.newArrayList("file1.parquet"))
                    .build();
            FakeExecutor fakeExecutor = buildExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));
            // When
            fakeExecutor.runJob(importJob);

            // Then
            assertThat(fakeExecutor.getJobsRun()).isEmpty();
            assertThat(ingestJobStatusStore.getJob("my-job")).isPresent().get()
                    .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                    .isEqualTo(jobStatus(importJob.toIngestJob(),
                            rejectedRun(importJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                    "The table name must be set to a non-null value.")));
        }

        @Test
        void shouldFailValidationIfJobIdContainsMoreThan63Characters() {
            // Given
            String invalidId = UUID.randomUUID().toString() + UUID.randomUUID();
            BulkImportJob importJob = new BulkImportJob.Builder()
                    .tableName(tableName)
                    .files(Lists.newArrayList("file1.parquet"))
                    .id(invalidId)
                    .build();
            FakeExecutor fakeExecutor = buildExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));
            // When
            fakeExecutor.runJob(importJob);

            // Then
            assertThat(fakeExecutor.getJobsRun()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableName))
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                    .containsExactly(jobStatus(importJob.toIngestJob(),
                            rejectedRun(importJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                    "Job IDs are only allowed to be up to 63 characters long.")));
        }

        @Test
        void shouldFailValidationIfJobIdContainsUppercaseLetters() {
            // Given
            BulkImportJob importJob = new BulkImportJob.Builder()
                    .tableName(tableName)
                    .id("importJob")
                    .files(Lists.newArrayList("file1.parquet"))
                    .build();
            FakeExecutor fakeExecutor = buildExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));

            // When
            fakeExecutor.runJob(importJob);

            // Then
            assertThat(fakeExecutor.getJobsRun()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs("myTable"))
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                    .containsExactly(jobStatus(importJob.toIngestJob(),
                            rejectedRun(importJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                    "Job Ids must only contain lowercase alphanumerics and dashes.")));
        }
    }

    @Test
    void shouldCallRunOnPlatformIfJobIsValid() {
        // Given
        s3.putObject(bucketName, "file1.parquet", "");
        s3.putObject(bucketName, "file2.parquet", "");
        s3.putObject(bucketName, "directory/file3.parquet", "");
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName(tableName)
                .id("my-job")
                .files(List.of(
                        bucketName + "/file1.parquet",
                        bucketName + "/file2.parquet",
                        bucketName + "/directory/file3.parquet"))
                .build();
        FakeExecutor fakeExecutor = buildExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));

        // When
        fakeExecutor.runJob(importJob, "job-run-id");

        // Then
        assertThat(fakeExecutor.getJobsRun()).containsExactly(importJob);
        assertThat(fakeExecutor.getJobRunIdsOfJobsRun()).containsExactly("job-run-id");
        assertThat(ingestJobStatusStore.getAllJobs("myTable"))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(importJob.toIngestJob(),
                        acceptedRun(importJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"))));
    }

    @Test
    void shouldSucceedIfS3ObjectIsADirectoryContainingFiles() {
        // Given
        s3.putObject(bucketName, "directory/file1.parquet", "");
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName(tableName)
                .id("my-job")
                .files(List.of(bucketName + "/directory", bucketName + "/directory/"))
                .build();
        FakeExecutor fakeExecutor = buildExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));

        // When
        fakeExecutor.runJob(importJob, "job-run-id");

        // Then
        assertThat(fakeExecutor.getJobsRun()).containsExactly(importJob);
        assertThat(fakeExecutor.getJobRunIdsOfJobsRun()).containsExactly("job-run-id");
        assertThat(ingestJobStatusStore.getAllJobs("myTable"))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(importJob.toIngestJob(),
                        acceptedRun(importJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"))));
    }

    @Test
    void shouldDoNothingWhenJobIsNull() {
        // Given
        FakeExecutor fakeExecutor = buildExecutorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));

        // When
        fakeExecutor.runJob(null);

        // Then
        assertThat(fakeExecutor.getJobsRun()).isEmpty();
    }

    private FakeExecutor buildExecutorWithValidationTime(Instant validationTime) {
        return new FakeExecutor(validationTime);
    }

    private class FakeExecutor implements PlatformExecutor {
        private final List<BulkImportJob> jobsRun = new ArrayList<>();
        private final List<String> jobRunIdsOfJobsRun = new ArrayList<>();
        private final Instant validationTime;

        FakeExecutor(Instant validationTime) {
            this.validationTime = validationTime;
        }

        public List<BulkImportJob> getJobsRun() {
            return jobsRun;
        }

        public List<String> getJobRunIdsOfJobsRun() {
            return jobRunIdsOfJobsRun;
        }

        public void runJob(BulkImportJob bulkImportJob) {
            bulkImportExecutor().runJob(bulkImportJob);
        }

        public void runJob(BulkImportJob bulkImportJob, String jobRunId) {
            bulkImportExecutor().runJob(bulkImportJob, jobRunId);
        }

        private BulkImportExecutor bulkImportExecutor() {
            TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
            StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tableProperties,
                    inMemoryStateStoreWithFixedSinglePartition(SCHEMA));
            return new BulkImportExecutor(instanceProperties, tablePropertiesProvider, stateStoreProvider,
                    ingestJobStatusStore, s3, this, List.of(validationTime).iterator()::next);
        }

        @Override
        public void runJobOnPlatform(BulkImportExecutor bulkImportExecutor, BulkImportJob bulkImportJob, String jobRunId) {
            jobsRun.add(bulkImportJob);
            jobRunIdsOfJobsRun.add(jobRunId);
        }

        @Override
        public String getJarLocation() {
            return "test-bucket";
        }
    }

}
