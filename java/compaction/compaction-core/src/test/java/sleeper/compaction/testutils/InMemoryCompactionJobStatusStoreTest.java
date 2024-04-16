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
package sleeper.compaction.testutils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobStatusFrom;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionStatus;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forJob;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forJobOnTask;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.records;
import static sleeper.core.record.process.status.TestRunStatusUpdates.defaultUpdateTime;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class InMemoryCompactionJobStatusStoreTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final String tableId = tableProperties.get(TABLE_ID);
    private final CompactionJobTestDataHelper dataHelper = CompactionJobTestDataHelper.forTable(instanceProperties, tableProperties);
    private final InMemoryCompactionJobStatusStore store = new InMemoryCompactionJobStatusStore();

    @Nested
    @DisplayName("Store status updates")
    class StoreStatusUpdates {

        @Test
        void shouldStoreCreatedJob() {
            // Given
            Instant storeTime = Instant.parse("2023-03-29T12:27:42Z");
            CompactionJob job = addCreatedJob(storeTime);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobCreated(job, storeTime));
        }

        @Test
        void shouldStoreStartedJob() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            String taskId = "test-task";
            CompactionJob job = addStartedJob(createdTime, startedTime, taskId);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getId(), taskId, startedCompactionStatus(startedTime)))));
        }

        @Test
        void shouldStoreFinishedJob() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant finishedTime = Instant.parse("2023-03-29T12:27:44Z");
            String taskId = "test-task";
            CompactionJob job = addFinishedJob(createdTime,
                    summary(startedTime, finishedTime, 100, 100), taskId);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getId(), taskId,
                                    startedCompactionStatus(startedTime),
                                    finishedCompactionStatus(summary(startedTime, finishedTime, 100, 100))))));
        }

        @Test
        void shouldUseDefaultUpdateTimeIfUpdateTimeNotFixed() {
            // Given
            Instant createdTime = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime = Instant.parse("2023-03-29T12:27:43Z");
            Instant finishTime = Instant.parse("2023-03-29T12:28:43Z");
            String taskId = "test-task";

            RecordsProcessedSummary summary = summary(startedTime, finishTime, 100L, 100L);
            CompactionJob job = dataHelper.singleFileCompaction();
            store.jobCreated(job, createdTime);
            store.jobStarted(job, startedTime, taskId);
            store.jobFinished(job, summary, taskId);

            // When / Then
            assertThat(store.streamAllJobs(tableId))
                    .containsExactly(jobStatusFrom(records().fromUpdates(
                            forJob(job.getId(), CompactionJobCreatedStatus.from(job, createdTime)),
                            forJobOnTask(job.getId(), taskId,
                                    startedCompactionStatus(startedTime),
                                    finishedCompactionStatus(summary)))));
        }
    }

    @Nested
    @DisplayName("Get job by ID")
    class GetJobById {
        @Test
        void shouldGetJobById() {
            // Given
            Instant storeTime = Instant.parse("2023-03-29T12:27:42Z");
            CompactionJob job = addCreatedJob(storeTime);

            // When / Then
            assertThat(store.getJob(job.getId()))
                    .contains(jobCreated(job, storeTime));
        }

        @Test
        void shouldFailToFindJobWhenIdDoesNotMatch() {
            // Given
            addCreatedJob(Instant.parse("2023-03-29T12:27:42Z"));

            // When / Then
            assertThat(store.getJob("not-a-job")).isEmpty();
        }

        @Test
        void shouldFailToFindJobWhenNonePresent() {
            assertThat(store.getJob("not-a-job")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Get all jobs")
    class GetAllJobs {

        @Test
        void shouldGetMultipleJobs() {
            // Given
            Instant time1 = Instant.parse("2023-03-29T12:27:42Z");
            CompactionJob job1 = addCreatedJob(time1);
            Instant time2 = Instant.parse("2023-03-29T12:27:43Z");
            CompactionJob job2 = addCreatedJob(time2);
            Instant time3 = Instant.parse("2023-03-29T12:27:44Z");
            CompactionJob job3 = addCreatedJob(time3);

            // When / Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(
                            jobCreated(job3, time3),
                            jobCreated(job2, time2),
                            jobCreated(job1, time1));
        }

        @Test
        void shouldGetNoJobs() {
            assertThat(store.getAllJobs("no-jobs-table"))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Get unfinished jobs")
    class GetUnfinishedJobs {
        @Test
        void shouldGetUnfinishedJobs() {
            // Given
            Instant createdTime1 = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime1 = Instant.parse("2023-03-29T12:27:43Z");
            String taskId1 = "test-task-1";
            CompactionJob job1 = addStartedJob(createdTime1, startedTime1, taskId1);

            Instant createdTime2 = Instant.parse("2023-03-29T13:27:42Z");
            Instant startedTime2 = Instant.parse("2023-03-29T13:27:43Z");
            Instant finishedTime2 = Instant.parse("2023-03-29T13:27:44Z");
            String taskId2 = "test-task-2";
            addFinishedJob(createdTime2, summary(startedTime2, finishedTime2, 100, 100), taskId2);

            // When / Then
            assertThat(store.getUnfinishedJobs(tableId))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job1.getId(), CompactionJobCreatedStatus.from(job1, createdTime1)),
                                    forJobOnTask(job1.getId(), taskId1, startedCompactionStatus(startedTime1)))));
        }

        @Test
        void shouldGetNoJobsWhenNoneUnfinished() {
            // Given
            addFinishedJob(Instant.parse("2023-03-29T15:10:12Z"),
                    summary(Instant.parse("2023-03-29T15:11:12Z"),
                            Instant.parse("2023-03-29T15:12:12Z"),
                            100, 100),
                    "test-task");

            // When / Then
            assertThat(store.getUnfinishedJobs(tableId)).isEmpty();
        }

        @Test
        void shouldGetNoJobsWhenNonePresent() {
            assertThat(store.getUnfinishedJobs(tableId)).isEmpty();
        }
    }

    @Nested
    @DisplayName("Get jobs by task ID")
    class GetJobsByTaskId {
        @Test
        void shouldGetJobsByTaskId() {
            // Given
            Instant createdTime1 = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime1 = Instant.parse("2023-03-29T12:27:43Z");
            String taskId1 = "test-task-1";
            CompactionJob job1 = addStartedJob(createdTime1, startedTime1, taskId1);

            Instant createdTime2 = Instant.parse("2023-03-29T13:27:42Z");
            Instant startedTime2 = Instant.parse("2023-03-29T13:27:43Z");
            String taskId2 = "test-task-2";
            addStartedJob(createdTime2, startedTime2, taskId2);

            // When / Then
            assertThat(store.getJobsByTaskId(tableId, taskId1))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job1.getId(), CompactionJobCreatedStatus.from(job1, createdTime1)),
                                    forJobOnTask(job1.getId(), taskId1, startedCompactionStatus(startedTime1)))));
        }

        @Test
        void shouldGetNoJobsWhenNoneForGivenTask() {
            // Given
            addFinishedJob(Instant.parse("2023-03-29T15:10:12Z"),
                    summary(Instant.parse("2023-03-29T15:11:12Z"),
                            Instant.parse("2023-03-29T15:12:12Z"),
                            100, 100),
                    "test-task");

            // When / Then
            assertThat(store.getJobsByTaskId(tableId, "other-task")).isEmpty();
        }

        @Test
        void shouldGetNoJobsWhenNonePresent() {
            assertThat(store.getJobsByTaskId(tableId, "some-task")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Get jobs in time period")
    class GetJobsInTimePeriod {
        @Test
        void shouldGetJobsInTimePeriod() {
            // Given
            Instant createdTime1 = Instant.parse("2023-03-29T12:27:42Z");
            Instant startedTime1 = Instant.parse("2023-03-29T12:27:43Z");
            String taskId1 = "test-task-1";
            CompactionJob job1 = addStartedJob(createdTime1, startedTime1, taskId1);

            Instant createdTime2 = Instant.parse("2023-03-29T13:27:42Z");
            Instant startedTime2 = Instant.parse("2023-03-29T13:27:43Z");
            String taskId2 = "test-task-2";
            addStartedJob(createdTime2, startedTime2, taskId2);

            // When / Then
            assertThat(store.getJobsInTimePeriod(tableId,
                    Instant.parse("2023-03-29T12:00:00Z"),
                    Instant.parse("2023-03-29T13:00:00Z")))
                    .containsExactly(
                            jobStatusFrom(records().fromUpdates(
                                    forJob(job1.getId(), CompactionJobCreatedStatus.from(job1, createdTime1)),
                                    forJobOnTask(job1.getId(), taskId1, startedCompactionStatus(startedTime1)))));
        }

        @Test
        void shouldGetNoJobsWhenNoneInGivenPeriod() {
            // Given
            addFinishedJob(Instant.parse("2023-03-29T15:10:12Z"),
                    summary(Instant.parse("2023-03-29T15:11:12Z"),
                            Instant.parse("2023-03-29T15:12:12Z"),
                            100, 100),
                    "test-task");

            // When / Then
            assertThat(store.getJobsInTimePeriod(tableId,
                    Instant.parse("2023-03-29T14:00:00Z"),
                    Instant.parse("2023-03-29T15:00:00Z"))).isEmpty();
        }

        @Test
        void shouldGetNoJobsWhenNonePresent() {
            assertThat(store.getJobsInTimePeriod(tableId,
                    Instant.parse("2023-03-29T14:00:00Z"),
                    Instant.parse("2023-03-29T15:00:00Z"))).isEmpty();
        }
    }

    private CompactionJob addCreatedJob(Instant createdTime) {
        CompactionJob job = dataHelper.singleFileCompaction();
        store.fixUpdateTime(createdTime);
        store.jobCreated(job);
        return job;
    }

    private CompactionJob addStartedJob(Instant createdTime, Instant startedTime, String taskId) {
        CompactionJob job = addCreatedJob(createdTime);
        store.fixUpdateTime(defaultUpdateTime(startedTime));
        store.jobStarted(job, startedTime, taskId);
        return job;
    }

    private CompactionJob addFinishedJob(Instant createdTime, RecordsProcessedSummary summary, String taskId) {
        CompactionJob job = addStartedJob(createdTime, summary.getStartTime(), taskId);
        store.fixUpdateTime(defaultUpdateTime(summary.getFinishTime()));
        store.jobFinished(job, summary, taskId);
        return job;
    }
}
