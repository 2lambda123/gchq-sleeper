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

package sleeper.ingest.status.store.task;

import org.junit.jupiter.api.Test;

import sleeper.ingest.status.store.testutils.DynamoDBIngestTaskStatusStoreTestBase;
import sleeper.ingest.task.IngestTaskStatus;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreIngestTaskIT extends DynamoDBIngestTaskStatusStoreTestBase {
    @Test
    public void shouldReportIngestTaskStarted() {
        // Given
        IngestTaskStatus taskStatus = startedTaskWithDefaults();

        // When
        store.taskStarted(taskStatus);

        // Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_EXPIRY_DATE)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportIngestTaskFinished() {
        // Given
        IngestTaskStatus taskStatus = finishedTaskWithDefaults();

        // When
        store.taskStarted(taskStatus);
        store.taskFinished(taskStatus);

        // Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_EXPIRY_DATE)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportIngestTaskFinishedWithDurationInSecondsNotAWholeNumber() {
        // Given
        IngestTaskStatus taskStatus = finishedTaskWithDefaultsAndDurationInSecondsNotAWholeNumber();

        // When
        store.taskStarted(taskStatus);
        store.taskFinished(taskStatus);

        // Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_EXPIRY_DATE)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportNoIngestTaskExistsInStore() {
        // Given
        IngestTaskStatus taskStatus = startedTaskWithDefaults();

        // When/Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_EXPIRY_DATE)
                .isNull();
    }

    @Test
    public void shouldReportIngestTaskFinishedWithZeroDuration() {
        // Given
        IngestTaskStatus taskStatus = finishedTaskWithNoJobsAndZeroDuration();

        // When
        store.taskStarted(taskStatus);
        store.taskFinished(taskStatus);

        // Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_EXPIRY_DATE)
                .isEqualTo(taskStatus);
    }

}
