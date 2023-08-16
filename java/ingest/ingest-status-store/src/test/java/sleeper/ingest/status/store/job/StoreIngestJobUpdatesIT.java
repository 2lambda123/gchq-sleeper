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

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.job.status.IngestJobStartedEvent.ingestJobStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestRun;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;

public class StoreIngestJobUpdatesIT extends DynamoDBIngestJobStatusStoreTestBase {

    @Test
    public void shouldReportIngestJobFinishedSeparatelyFromStarted() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:51:42.001Z");

        // When
        store.jobStarted(defaultJobStartedEvent(job, startedTime));
        store.jobFinished(defaultJobFinishedEvent(job, startedTime, finishedTime));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobFinishedStatus(job, startedTime, finishedTime));
    }

    @Test
    public void shouldReportLatestUpdatesWhenJobIsRunMultipleTimes() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startTime1 = Instant.parse("2022-10-03T15:19:01.001Z");
        Instant finishTime1 = Instant.parse("2022-10-03T15:19:31.001Z");
        Instant startTime2 = Instant.parse("2022-10-03T15:19:02.001Z");
        Instant finishTime2 = Instant.parse("2022-10-03T15:19:32.001Z");
        String taskId1 = "first-task";
        String taskId2 = "second-task";

        // When
        store.jobStarted(ingestJobStarted(taskId1, job, startTime1));
        store.jobStarted(ingestJobStarted(taskId2, job, startTime2));
        store.jobFinished(ingestJobFinished(taskId1, job, defaultSummary(startTime1, finishTime1)));
        store.jobFinished(ingestJobFinished(taskId2, job, defaultSummary(startTime2, finishTime2)));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job,
                        finishedIngestRun(job, taskId2, defaultSummary(startTime2, finishTime2)),
                        finishedIngestRun(job, taskId1, defaultSummary(startTime1, finishTime1))));
    }

    @Test
    void shouldClearStatusStoreAfterJobFinished() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:51:42.001Z");
        store.jobStarted(defaultJobStartedEvent(job, startedTime));
        store.jobFinished(defaultJobFinishedEvent(job, startedTime, finishedTime));

        // When
        store.clear();

        // Then
        assertThat(getAllJobStatuses()).isEmpty();
    }
}
