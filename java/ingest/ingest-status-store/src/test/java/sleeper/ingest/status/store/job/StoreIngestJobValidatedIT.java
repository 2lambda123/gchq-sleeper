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

package sleeper.ingest.status.store.job;

import org.junit.jupiter.api.Test;

import sleeper.ingest.job.IngestJob;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobStatusStoreTestBase;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.job.status.IngestJobStartedEvent.validatedIngestJobStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestData.acceptedRunOnTask;
import static sleeper.ingest.job.status.IngestJobStatusTestData.acceptedRunWhichStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.rejectedRun;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobRejected;

public class StoreIngestJobValidatedIT extends DynamoDBIngestJobStatusStoreTestBase {
    @Test
    void shouldReportUnstartedJobWithNoValidationFailures() {
        // Given
        String tableName = "test-table";
        String taskId = "some-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

        // When
        store.jobValidated(ingestJobAccepted(job, validationTime).taskId(taskId).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, acceptedRunOnTask(taskId, validationTime)));
    }

    @Test
    void shouldReportStartedJobWithNoValidationFailures() {
        // Given
        String tableName = "test-table";
        String taskId = "some-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
        Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");

        // When
        store.jobValidated(ingestJobAccepted(job, validationTime).taskId(taskId).build());
        store.jobStarted(validatedIngestJobStarted(job, startTime).taskId(taskId).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, acceptedRunWhichStarted(job, taskId,
                        validationTime, startTime)));
    }

    @Test
    void shouldReportJobWithOneValidationFailure() {
        // Given
        String tableName = "test-table";
        IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

        // When
        store.jobValidated(ingestJobRejected(job, validationTime, "Test validation reason"));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, rejectedRun(
                        validationTime, "Test validation reason")));
    }

    @Test
    void shouldReportJobWithMultipleValidationFailures() {
        // Given
        String tableName = "test-table";
        IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

        // When
        store.jobValidated(ingestJobRejected(job, validationTime,
                "Test validation reason 1", "Test validation reason 2"));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, rejectedRun(validationTime,
                        List.of("Test validation reason 1", "Test validation reason 2"))));
    }
}
