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

package sleeper.clients.admin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.clients.admin.testutils.RunAdminClient;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.testutils.CompactionJobStatusStoreInMemory;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.COMPACTION_JOB_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.COMPACTION_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.COMPACTION_STATUS_STORE_NOT_ENABLED_MESSAGE;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.COMPACTION_TASK_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.DISPLAY_MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_ALL_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_DETAILED_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_RANGE_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.JOB_QUERY_UNFINISHED_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TASK_QUERY_ALL_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TASK_QUERY_UNFINISHED_OPTION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_STATUS_STORE_ENABLED;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.console.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.status.report.compaction.task.CompactionTaskStatusReportTestHelper.startedTask;

class CompactionStatusReportScreenTest extends AdminClientMockStoreBase {
    @Nested
    @DisplayName("Compaction job status report")
    class CompactionJobStatusReport {
        private final CompactionJobStatusStoreInMemory statusStore = new CompactionJobStatusStoreInMemory();
        private CompactionJob exampleJob;

        @BeforeEach
        void setUp() {
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties tableProperties = createValidTableProperties(properties, "test-table");
            setInstanceProperties(properties, tableProperties);
            exampleJob = new CompactionJobTestDataHelper(properties, tableProperties).singleFileCompaction();
            statusStore.fixTime(Instant.parse("2023-03-15T17:52:12.001Z"));
            statusStore.jobCreated(exampleJob);
            statusStore.fixTime(Instant.parse("2023-03-15T17:53:12.123Z"));
            statusStore.jobStarted(exampleJob, Instant.parse("2023-03-15T17:53:12.001Z"), "test-task-1");
        }

        @Test
        void shouldRunCompactionJobStatusReportWithQueryTypeAll() throws Exception {
            // When
            String output = runCompactionJobStatusReport()
                    .enterPrompts(JOB_QUERY_ALL_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Compaction Job Status Report\n" +
                            "----------------------------\n" +
                            "Total jobs: 1\n" +
                            "\n" +
                            "Total standard jobs: 1\n" +
                            "Total standard jobs pending: 0\n" +
                            "Total standard jobs in progress: 1\n" +
                            "Total standard jobs finished: 0");

            verifyWithNumberOfInvocations(4);
        }

        @Test
        void shouldRunCompactionJobStatusReportWithQueryTypeUnfinished() throws Exception {
            // When
            String output = runCompactionJobStatusReport()
                    .enterPrompts(JOB_QUERY_UNFINISHED_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Compaction Job Status Report\n" +
                            "----------------------------\n" +
                            "Total unfinished jobs: 1\n" +
                            "Total unfinished jobs in progress: 1\n" +
                            "Total unfinished jobs not started: 0");

            verifyWithNumberOfInvocations(4);
        }

        @Test
        void shouldRunCompactionJobStatusReportWithQueryTypeDetailed() throws Exception {
            // When
            String output = runCompactionJobStatusReport()
                    .enterPrompts(JOB_QUERY_DETAILED_OPTION, exampleJob.getId(), CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Compaction Job Status Report\n" +
                            "----------------------------\n" +
                            "Details for job " + exampleJob.getId());

            verifyWithNumberOfInvocations(5);
        }

        @Test
        void shouldRunCompactionJobStatusReportWithQueryTypeRange() throws Exception {
            // When
            String output = runCompactionJobStatusReport()
                    .enterPrompts(JOB_QUERY_RANGE_OPTION, "20230310175212", "20230318175212", CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Compaction Job Status Report\n" +
                            "----------------------------\n" +
                            "Total jobs in defined range: 1");

            verifyWithNumberOfInvocations(6);
        }

        private RunAdminClient runCompactionJobStatusReport() {
            return runClient().enterPrompts(COMPACTION_STATUS_REPORT_OPTION,
                            COMPACTION_JOB_STATUS_REPORT_OPTION, "test-table")
                    .statusStore(statusStore);
        }
    }

    @Nested
    @DisplayName("Compaction task status report")
    class CompactionTaskStatusReport {
        private final CompactionTaskStatusStore compactionTaskStatusStore = mock(CompactionTaskStatusStore.class);

        private List<CompactionTaskStatus> exampleTaskStatuses() {
            return List.of(startedTask("task-1", "2023-03-15T18:53:12.001Z"));
        }

        @Test
        void shouldRunCompactionTaskStatusReportWithQueryTypeAll() throws Exception {
            // Given
            when(compactionTaskStatusStore.getAllTasks())
                    .thenReturn(exampleTaskStatuses());

            // When/Then
            String output = runCompactionTaskStatusReport()
                    .enterPrompts(TASK_QUERY_ALL_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Compaction Task Status Report\n" +
                            "-----------------------------\n" +
                            "Total tasks: 1\n" +
                            "\n" +
                            "Total standard tasks: 1\n" +
                            "Total standard tasks in progress: 1\n" +
                            "Total standard tasks finished: 0\n" +
                            "\n" +
                            "Total splitting tasks: 0\n" +
                            "Total splitting tasks in progress: 0\n" +
                            "Total splitting tasks finished: 0\n");

            verifyWithNumberOfInvocations(3);
        }

        @Test
        void shouldRunCompactionTaskStatusReportWithQueryTypeUnfinished() throws Exception {
            // Given
            when(compactionTaskStatusStore.getTasksInProgress())
                    .thenReturn(exampleTaskStatuses());

            // When/Then
            String output = runCompactionTaskStatusReport()
                    .enterPrompts(TASK_QUERY_UNFINISHED_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();
            assertThat(output)
                    .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE)
                    .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                    .contains("" +
                            "Compaction Task Status Report\n" +
                            "-----------------------------\n" +
                            "Total tasks in progress: 1\n" +
                            "Total standard tasks in progress: 1\n" +
                            "Total splitting tasks in progress: 0\n");

            verifyWithNumberOfInvocations(3);
        }

        private RunAdminClient runCompactionTaskStatusReport() {
            InstanceProperties properties = createValidInstanceProperties();
            setInstanceProperties(properties);
            return runClient().enterPrompts(COMPACTION_STATUS_REPORT_OPTION, COMPACTION_TASK_STATUS_REPORT_OPTION)
                    .statusStore(compactionTaskStatusStore);
        }
    }

    @Test
    void shouldReturnToMainMenuIfCompactionStatusStoreNotEnabled() throws Exception {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        properties.set(COMPACTION_STATUS_STORE_ENABLED, "false");
        setInstanceProperties(properties);

        // When
        String output = runClient()
                .enterPrompts(COMPACTION_STATUS_REPORT_OPTION, CONFIRM_PROMPT)
                .exitGetOutput();

        // Then
        assertThat(output)
                .isEqualTo(DISPLAY_MAIN_SCREEN +
                        COMPACTION_STATUS_STORE_NOT_ENABLED_MESSAGE +
                        PROMPT_RETURN_TO_MAIN + DISPLAY_MAIN_SCREEN);
        verifyWithNumberOfInvocations(1);
    }

    private void verifyWithNumberOfInvocations(int numberOfInvocations) {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(numberOfInvocations)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
