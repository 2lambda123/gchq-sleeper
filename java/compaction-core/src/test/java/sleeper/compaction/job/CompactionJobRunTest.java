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

package sleeper.compaction.job;

import org.junit.Test;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStartedStatus;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobStatusFromUpdates;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.DEFAULT_TASK_ID;
import static sleeper.core.record.process.status.TestRunStatusUpdates.finishedStatus;
import static sleeper.core.record.process.status.TestRunStatusUpdates.startedStatus;

public class CompactionJobRunTest {

    @Test
    public void shouldReportNoRunsWhenJobNotStarted() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();

        // When
        CompactionJobStatus status = jobStatusFromUpdates(created);

        // Then
        assertThat(status.getJobRuns())
                .isEmpty();
        assertThat(status.isFinished()).isFalse();
    }

    @Test
    public void shouldReportNoFinishedStatusWhenJobNotFinished() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

        // When
        CompactionJobStatus status = jobStatusFromUpdates(created, started);

        // Then
        assertThat(status.getJobRuns())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, null));
        assertThat(status.isFinished()).isFalse();
    }

    @Test
    public void shouldReportRunWhenJobFinished() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished = finishedStatus(started, Duration.ofSeconds(30), 450L, 300L);

        // When
        CompactionJobStatus status = jobStatusFromUpdates(created, started, finished);

        // Then
        assertThat(status.getJobRuns())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, finished));
        assertThat(status.isFinished()).isTrue();
    }
}
