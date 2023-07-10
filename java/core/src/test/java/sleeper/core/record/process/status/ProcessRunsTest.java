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

package sleeper.core.record.process.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.core.record.process.status.CustomProcessStatus.notPartOfRunWithUpdateTime;
import static sleeper.core.record.process.status.CustomProcessStatus.partOfRunWithUpdateTime;
import static sleeper.core.record.process.status.ProcessStartedStatusWithStartOfRunFlag.startedStatusNotStartOfRun;
import static sleeper.core.record.process.status.TestProcessRuns.runsFromUpdates;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.DEFAULT_TASK_ID;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.TASK_ID_1;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.TASK_ID_2;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forRunOnNoTask;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forRunOnTask;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.onNoTask;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.onTask;
import static sleeper.core.record.process.status.TestRunStatusUpdates.finishedStatus;
import static sleeper.core.record.process.status.TestRunStatusUpdates.startedStatus;

class ProcessRunsTest {

    @DisplayName("Report start and finish of a process")
    @Nested
    class ReportStartAndFinish {

        @Test
        void shouldReportNoFinishedStatusWhenJobNotFinished() {
            // Given
            ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

            // When
            ProcessRuns runs = runsFromUpdates(started);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started, null));
            assertThat(runs.isFinished()).isFalse();
        }

        @Test
        void shouldReportRunWhenJobFinished() {
            // Given
            ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            ProcessFinishedStatus finished = finishedStatus(started, Duration.ofSeconds(30), 450L, 300L);

            // When
            ProcessRuns runs = runsFromUpdates(started, finished);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started, finished));
            assertThat(runs.isFinished()).isTrue();
        }

        @Test
        void shouldIgnoreExtraFinishedStatus() {
            // Given
            ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            ProcessFinishedStatus finished1 = finishedStatus(started, Duration.ofSeconds(30), 100, 100);
            ProcessFinishedStatus finished2 = finishedStatus(started, Duration.ofSeconds(40), 200, 200);

            // When
            ProcessRuns runs = runsFromUpdates(started, finished1, finished2);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started, finished1));
            assertThat(runs.isFinished()).isTrue();
        }
    }

    @DisplayName("Correlate process runs by task ID and time")
    @Nested
    class CorrelateRunsByTaskAndTime {

        @Test
        void shouldReportTwoRunsLatestFirstByStartTimeOnSameTask() {
            // Given
            ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:24:30.001Z"));

            // When
            ProcessRuns runs = runsFromUpdates(started1, started2);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started2, null),
                            tuple(DEFAULT_TASK_ID, started1, null));
            assertThat(runs.isFinished()).isFalse();
        }

        @Test
        void shouldReportTwoRunsWhenJobFinishedMultipleTimesSameTask() {
            // Given
            ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
            ProcessFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 450L, 300L);
            ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            ProcessFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

            // When
            ProcessRuns runs = runsFromUpdates(started1, finished1, started2, finished2);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started2, finished2),
                            tuple(DEFAULT_TASK_ID, started1, finished1));
            assertThat(runs.isFinished()).isTrue();
        }

        @Test
        void shouldReportTwoTasksWithTwoRunsEachForSameJobWithInterleavingStartTimes() {
            // Given
            ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
            ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            ProcessStartedStatus started3 = startedStatus(Instant.parse("2022-09-25T09:23:30.001Z"));
            ProcessStartedStatus started4 = startedStatus(Instant.parse("2022-09-26T09:23:30.001Z"));

            // When
            ProcessRuns runs = runsFromUpdates(
                    onTask(TASK_ID_1, started1, started3),
                    onTask(TASK_ID_2, started2, started4));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                    .containsExactly(
                            tuple(TASK_ID_2, started4, null),
                            tuple(TASK_ID_1, started3, null),
                            tuple(TASK_ID_2, started2, null),
                            tuple(TASK_ID_1, started1, null));
            assertThat(runs.isFinished()).isFalse();
        }

        @Test
        void shouldReportTwoTasksWithOneFinishedRunEach() {
            // Given
            ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
            ProcessFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 450L, 300L);
            ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            ProcessFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

            // When
            ProcessRuns runs = runsFromUpdates(
                    onTask(TASK_ID_1, started1, finished1),
                    onTask(TASK_ID_2, started2, finished2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                    .containsExactly(
                            tuple(TASK_ID_2, started2, finished2),
                            tuple(TASK_ID_1, started1, finished1));
            assertThat(runs.isFinished()).isTrue();
        }

        @Test
        void shouldReportRunsOnDifferentTasksWhenJobRunStartedAndFinishedDuringAnotherRun() {
            // Given
            ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:00.001Z"));
            ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
            ProcessFinishedStatus finished1 = finishedStatus(started1, Duration.ofMinutes(2), 450L, 300L);
            ProcessFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

            // When
            ProcessRuns runs = runsFromUpdates(
                    onTask(TASK_ID_1, started1, finished1),
                    onTask(TASK_ID_2, started2, finished2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                    .containsExactly(
                            tuple(TASK_ID_2, started2, finished2),
                            tuple(TASK_ID_1, started1, finished1));
            assertThat(runs.isFinished()).isTrue();
        }
    }

    @DisplayName("Correlate process runs by run ID")
    @Nested
    class CorrelateRunsById {

        @Test
        void shouldReportTwoRunsLatestFirstWhenAnEventHappensForBothBeforeEitherAreOnATask() {
            // Given
            ProcessStartedStatus validated1 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            ProcessStartedStatus validated2 = startedStatus(Instant.parse("2022-09-24T09:24:30.001Z"));

            ProcessStartedStatusWithStartOfRunFlag started1 = startedStatusNotStartOfRun(Instant.parse("2022-09-24T10:23:30Z"));
            ProcessStartedStatusWithStartOfRunFlag started2 = startedStatusNotStartOfRun(Instant.parse("2022-09-24T10:24:30Z"));

            // When
            ProcessRuns runs = runsFromUpdates(
                    forRunOnNoTask("run-1", validated1),
                    forRunOnNoTask("run-2", validated2),
                    forRunOnTask("run-1", "some-task", started1),
                    forRunOnTask("run-2", "some-task", started2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", List.of(validated2, started2)),
                            tuple("some-task", List.of(validated1, started1)));
            assertThat(runs.isFinished()).isFalse();
        }

        @Test
        void shouldIgnoreExtraFinishedStatus() {
            // Given
            ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            ProcessFinishedStatus finished1 = finishedStatus(started, Duration.ofSeconds(30), 100, 100);
            ProcessFinishedStatus finished2 = finishedStatus(started, Duration.ofSeconds(40), 200, 200);

            // When
            ProcessRuns runs = runsFromUpdates(
                    forRunOnTask("run-1", "some-task", started),
                    forRunOnTask("run-1", "some-task", finished1),
                    forRunOnTask("run-1", "some-task", finished2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                    .containsExactly(
                            tuple("some-task", started, finished1));
            assertThat(runs.isFinished()).isTrue();
        }
    }

    @DisplayName("Report task assignment")
    @Nested
    class ReportTaskAssignment {

        @Test
        void shouldReportAssignedToTask() {
            // Given
            ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

            // When
            ProcessRuns runs = runsFromUpdates(onTask("some-task", started));

            // Then
            assertThat(runs.isTaskIdAssigned("some-task"))
                    .isTrue();
        }

        @Test
        void shouldReportNotAssignedToTaskWhenOnAnotherTask() {
            // Given
            ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

            // When
            ProcessRuns runs = runsFromUpdates(onTask("other-task", started));

            // Then
            assertThat(runs.isTaskIdAssigned("some-task"))
                    .isFalse();
        }

        @Test
        void shouldReportNotAssignedToTaskWhenOnNoTask() {
            // Given
            ProcessStartedStatus validated = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

            // When
            ProcessRuns runs = runsFromUpdates(onNoTask(validated));

            // Then
            assertThat(runs.isTaskIdAssigned("some-task"))
                    .isFalse();
        }
    }

    @DisplayName("Flag updates as part/start of a run")
    @Nested
    class FlagUpdatesAsPartOrStartOfRun {

        @Test
        void shouldNotCreateProcessRunIfStatusUpdateNotFlaggedAsStartOfRun() {
            // Given
            ProcessStatusUpdate notStartedUpdate = () -> Instant.parse("2022-09-24T09:23:30.001Z");

            // When
            ProcessRuns runs = runsFromUpdates(notStartedUpdate);

            // Then
            assertThat(runs.getRunsLatestFirst()).isEmpty();
        }

        @Test
        void shouldCreateProcessRunFromTwoStartedUpdatesWhenStartOfRunIsAfterTheOther() {
            // Given
            ProcessStartedStatusWithStartOfRunFlag startedStatusNotStartOfRun = startedStatusNotStartOfRun(
                    Instant.parse("2022-09-24T08:23:30Z"));
            ProcessStartedStatus startedStatus = startedStatus(
                    Instant.parse("2022-09-24T09:23:30Z"));

            // When
            ProcessRuns runs = runsFromUpdates(startedStatusNotStartOfRun, startedStatus);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .containsExactly(ProcessRun.started(DEFAULT_TASK_ID, startedStatus));
        }

        @Test
        void shouldCreateProcessRunFromTwoStartedUpdatesWhenStartOfRunIsBeforeTheOther() {
            // Given
            ProcessStartedStatus startedStatus = startedStatus(
                    Instant.parse("2022-09-24T09:23:30Z"));
            ProcessStartedStatusWithStartOfRunFlag startedStatusNotStartOfRun = startedStatusNotStartOfRun(
                    Instant.parse("2022-09-24T10:23:30Z"));

            // When
            ProcessRuns runs = runsFromUpdates(startedStatus, startedStatusNotStartOfRun);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .containsExactly(ProcessRun.builder()
                            .taskId(DEFAULT_TASK_ID)
                            .startedStatus(startedStatus)
                            .statusUpdate(startedStatusNotStartOfRun)
                            .build());
        }

        @Test
        void shouldCreateProcessRunWithCustomStatusUpdatePartOfRun() {
            // Given
            ProcessStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            CustomProcessStatus customStatus = partOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));

            // When
            ProcessRuns runs = runsFromUpdates(startedStatus, customStatus);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .containsExactly(ProcessRun.builder()
                            .taskId(DEFAULT_TASK_ID)
                            .startedStatus(startedStatus)
                            .statusUpdate(customStatus)
                            .build());
        }

        @Test
        void shouldCreateProcessRunWithCustomStatusUpdateNotPartOfRun() {
            // Given
            ProcessStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            CustomProcessStatus customStatus = notPartOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));

            // When
            ProcessRuns runs = runsFromUpdates(startedStatus, customStatus);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .containsExactly(ProcessRun.started(DEFAULT_TASK_ID, startedStatus));
        }
    }

    @Nested
    @DisplayName("Retrieve status updates by class")
    class RetrieveStatusUpdatesByClass {
        @Test
        void shouldReturnLastStatusUpdateByClass() {
            // Given
            ProcessStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            CustomProcessStatus customStatus = partOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));

            // When
            ProcessRuns runs = runsFromUpdates(startedStatus, customStatus);

            // Then
            assertThat(runs.getLatestRun()
                    .flatMap(latestRun -> latestRun.getLastStatusOfType(CustomProcessStatus.class)))
                    .get().isEqualTo(customStatus);
        }

        @Test
        void shouldReturnLastStatusUpdateByClassWithMultipleUpdatesForClass() {
            // Given
            ProcessStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            CustomProcessStatus customStatus1 = partOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));
            CustomProcessStatus customStatus2 = partOfRunWithUpdateTime(Instant.parse("2022-09-24T10:25:30Z"));

            // When
            ProcessRuns runs = runsFromUpdates(startedStatus, customStatus1, customStatus2);

            // Then
            assertThat(runs.getLatestRun()
                    .flatMap(latestRun -> latestRun.getLastStatusOfType(CustomProcessStatus.class)))
                    .get().isEqualTo(customStatus2);
        }

        @Test
        void shouldReturnLastStatusUpdateByInterface() {
            ProcessStartedStatus startedUpdate = startedStatus(
                    Instant.parse("2022-09-24T09:23:30Z"));
            ProcessStartedStatusWithStartOfRunFlag startedStatusNotStartOfRun = startedStatusNotStartOfRun(
                    Instant.parse("2022-09-24T10:23:30Z"));

            // When
            ProcessRuns runs = runsFromUpdates(startedUpdate, startedStatusNotStartOfRun);

            // Then
            assertThat(runs.getLatestRun()
                    .flatMap(latestRun -> latestRun.getLastStatusOfType(ProcessRunStartedUpdate.class)))
                    .get().isEqualTo(startedStatusNotStartOfRun);
        }
    }
}
