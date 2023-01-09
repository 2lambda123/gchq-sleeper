/*
 * Copyright 2023 Crown Copyright
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

import org.junit.Test;

import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobStatusStoreTestBase;

import java.time.Instant;
import java.time.Period;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestJob;

public class QueryIngestJobStatusByPeriodIT extends DynamoDBIngestJobStatusStoreTestBase {

    @Test
    public void shouldReturnIngestJobsInPeriod() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithFiles("file2");
        Instant startedTime1 = Instant.parse("2023-01-03T14:50:00.001Z");
        Instant startedUpdateTime1 = Instant.parse("2023-01-03T14:50:00.123Z");
        Instant startedTime2 = Instant.parse("2023-01-03T14:55:00.001Z");
        Instant startedUpdateTime2 = Instant.parse("2023-01-03T14:55:00.123Z");
        IngestJobStatusStore store = storeWithUpdateTimes(startedUpdateTime1, startedUpdateTime2);

        // When
        store.jobStarted(DEFAULT_TASK_ID, job1, startedTime1);
        store.jobStarted(DEFAULT_TASK_ID, job2, startedTime2);

        // Then
        Instant epochStart = Instant.ofEpochMilli(0);
        Instant farFuture = epochStart.plus(Period.ofDays(999999999));
        assertThat(store.getJobsInTimePeriod(tableName, epochStart, farFuture))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        startedIngestJob(job2, DEFAULT_TASK_ID, startedTime2),
                        startedIngestJob(job1, DEFAULT_TASK_ID, startedTime1));
    }

    @Test
    public void shouldExcludeIngestJobOutsidePeriod() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant periodStart = Instant.parse("2023-01-01T14:00:00.001Z");
        Instant periodEnd = Instant.parse("2023-01-02T14:00:00.001Z");
        Instant startedTime = Instant.parse("2023-01-03T14:50:00.001Z");
        Instant startedUpdateTime = Instant.parse("2023-01-03T14:50:00.123Z");
        IngestJobStatusStore store = storeWithUpdateTimes(startedUpdateTime);

        // When
        store.jobStarted(DEFAULT_TASK_ID, job, startedTime);

        // Then
        assertThat(store.getJobsInTimePeriod(tableName, periodStart, periodEnd)).isEmpty();
    }

    @Test
    public void shouldExcludeIngestJobInOtherTable() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithTableAndFiles("other-table", "file2");
        Instant startedTime1 = Instant.parse("2023-01-03T14:50:00.001Z");
        Instant startedUpdateTime1 = Instant.parse("2023-01-03T14:50:00.123Z");
        Instant startedTime2 = Instant.parse("2023-01-03T14:55:00.001Z");
        Instant startedUpdateTime2 = Instant.parse("2023-01-03T14:55:00.123Z");
        IngestJobStatusStore store = storeWithUpdateTimes(startedUpdateTime1, startedUpdateTime2);

        // When
        store.jobStarted(DEFAULT_TASK_ID, job1, startedTime1);
        store.jobStarted(DEFAULT_TASK_ID, job2, startedTime2);

        // Then
        Instant epochStart = Instant.ofEpochMilli(0);
        Instant farFuture = epochStart.plus(Period.ofDays(999999999));
        assertThat(store.getJobsInTimePeriod(tableName, epochStart, farFuture))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(startedIngestJob(job1, DEFAULT_TASK_ID, startedTime1));
    }

    @Test
    public void shouldIncludeFinishedStatusUpdateOutsidePeriod() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant periodStart = Instant.parse("2023-01-02T14:52:00.001Z");
        Instant startedTime = Instant.parse("2023-01-03T14:50:00.001Z");
        Instant startedUpdateTime = Instant.parse("2023-01-03T14:50:00.123Z");
        Instant periodEnd = Instant.parse("2023-01-03T14:52:00.001Z");
        Instant finishedTime = Instant.parse("2023-01-03T14:56:00.001Z");
        Instant finishedUpdateTime = Instant.parse("2023-01-03T14:56:00.123Z");
        IngestJobStatusStore store = storeWithUpdateTimes(startedUpdateTime, finishedUpdateTime);

        // When
        store.jobStarted(DEFAULT_TASK_ID, job, startedTime);
        store.jobFinished(DEFAULT_TASK_ID, job, defaultSummary(startedTime, finishedTime));

        // Then
        assertThat(store.getJobsInTimePeriod(tableName, periodStart, periodEnd))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(finishedIngestJob(job, DEFAULT_TASK_ID, defaultSummary(startedTime, finishedTime)));
    }

}
