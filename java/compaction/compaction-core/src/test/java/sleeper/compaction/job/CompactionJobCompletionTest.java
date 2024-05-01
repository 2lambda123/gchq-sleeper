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
package sleeper.compaction.job;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;

public class CompactionJobCompletionTest extends CompactionJobCompletionTestBase {

    @Nested
    @DisplayName("Retry state store update")
    class RetryStateStoreUpdate {

        @BeforeEach
        void setUp() {
            createTable();
        }

        @Test
        void shouldRetryStateStoreUpdateWhenFilesNotAssignedToJob() throws Exception {
            // Given
            FileReference file = addInputFile("file.parquet", 123);
            CompactionJob job = createCompactionJobForOneFileNoJobAssignment(file);
            CompactionJobRunCompleted completion = runCompactionJobOnTask("test-task", job);
            actionOnWait(() -> {
                stateStore().assignJobIds(List.of(AssignJobIdRequest.assignJobOnPartitionToFiles(
                        job.getId(), file.getPartitionId(), List.of(file.getFilename()))));
            });
            Instant updateTime = Instant.parse("2024-05-01T10:59:30Z");
            stateStore().fixFileUpdateTime(updateTime);

            // When
            jobCompletion(noJitter()).applyCompletedJob(completion);

            // Then
            assertThat(stateStore().getFileReferences()).containsExactly(
                    FileReferenceFactory.fromUpdatedAt(stateStore(), updateTime)
                            .rootFile(job.getOutputFile(), 100));
            assertThat(foundWaits).containsExactly(Duration.ofSeconds(2));
        }

        @Test
        void shouldFailAfterMaxAttemptsWhenFilesNotAssignedToJob() throws Exception {
            // Given
            FileReference file = addInputFile("file.parquet", 123);
            CompactionJob job = createCompactionJobForOneFileNoJobAssignment(file);
            CompactionJobRunCompleted completion = runCompactionJobOnTask("test-task", job);

            // When
            assertThatThrownBy(() -> jobCompletion(noJitter()).applyCompletedJob(completion))
                    .isInstanceOf(TimedOutWaitingForFileAssignmentsException.class)
                    .hasCauseInstanceOf(FileReferenceNotAssignedToJobException.class);

            // Then
            assertThat(stateStore().getFileReferences()).containsExactly(file);
            assertThat(foundWaits).containsExactly(
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(4),
                    Duration.ofSeconds(8),
                    Duration.ofSeconds(16),
                    Duration.ofSeconds(32),
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(1));
        }

        @Test
        void shouldFailWithNoRetriesWhenFileDoesNotExistInStateStore() throws Exception {
            // Given
            FileReference file = inputFileFactory().rootFile("file.parquet", 123);
            CompactionJob job = createCompactionJobForOneFileNoJobAssignment(file);
            CompactionJobRunCompleted completion = runCompactionJobOnTask("test-task", job);

            // When
            assertThatThrownBy(() -> jobCompletion(noJitter()).applyCompletedJob(completion))
                    .isInstanceOf(FileNotFoundException.class);

            // Then
            assertThat(stateStore().getFileReferences()).isEmpty();
            assertThat(foundWaits).isEmpty();
        }
    }
}
