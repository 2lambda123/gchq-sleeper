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

import org.junit.Test;

import sleeper.ingest.job.IngestJob;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobStatusStoreTestBase;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestJob;

public class StoreIngestJobStartedIT extends DynamoDBIngestJobStatusStoreTestBase {

    @Test
    public void shouldReportIngestJobStarted() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");

        // When
        store.jobStarted(DEFAULT_TASK_ID, job, startedTime);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(startedIngestJob(job, DEFAULT_TASK_ID, startedTime));
    }

    @Test
    public void shouldReportIngestJobStartedWithSeveralFiles() {
        // Given
        IngestJob job = jobWithFiles("file1", "file2");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");

        // When
        store.jobStarted(DEFAULT_TASK_ID, job, startedTime);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(startedIngestJob(job, DEFAULT_TASK_ID, startedTime));
    }

    @Test
    public void shouldReportSeveralIngestJobsStarted() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithFiles("file2");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:56:12.001Z");

        // When
        store.jobStarted(DEFAULT_TASK_ID, job1, startedTime1);
        store.jobStarted(DEFAULT_TASK_ID, job2, startedTime2);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        startedIngestJob(job2, DEFAULT_TASK_ID, startedTime2),
                        startedIngestJob(job1, DEFAULT_TASK_ID, startedTime1));
    }

}
