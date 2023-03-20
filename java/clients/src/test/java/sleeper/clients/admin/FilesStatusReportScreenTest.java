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

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.statestore.StateStore;
import sleeper.statestore.inmemory.StateStoreTestBuilder;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.FILES_STATUS_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.RETURN_TO_MAIN_SCREEN_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.status.report.partitions.PartitionStatusReportTestHelper.createPartitionsBuilder;

class FilesStatusReportScreenTest extends AdminClientMockStoreBase {
    private final StateStore stateStore = StateStoreTestBuilder.from(createPartitionsBuilder()
                    .leavesWithSplits(Arrays.asList("A", "B"), List.of("aaa"))
                    .parentJoining("parent", "A", "B"))
            .singleFileInEachLeafPartitionWithRecords(5)
            .buildStateStore();

    @Test
    void shouldRunFilesStatusReportWithDefaultArgs() throws Exception {
        // Given
        setStateStoreForTable("test-table", stateStore);
        in.enterNextPrompts(FILES_STATUS_REPORT_OPTION,
                "test-table", "", "", EXIT_OPTION);

        // When/Then
        String output = runClientGetOutput();
        assertThat(output).startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE + TABLE_SELECT_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("No value entered, defaulting to 1000")
                .contains("" +
                        "Files Status Report:\n" +
                        "--------------------------\n" +
                        "There are 2 leaf partitions and 1 non-leaf partitions");
        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldRunFilesStatusReportWithVerboseOption() throws Exception {
        // Given
        setStateStoreForTable("test-table", stateStore);
        in.enterNextPrompts(FILES_STATUS_REPORT_OPTION,
                "test-table", "", "y", EXIT_OPTION);

        // When/Then
        String output = runClientGetOutput();
        assertThat(output).startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE + TABLE_SELECT_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("No value entered, defaulting to 1000")
                .contains("" +
                        "Files Status Report:\n" +
                        "--------------------------\n" +
                        "There are 2 leaf partitions and 1 non-leaf partitions")
                .contains("" +
                        "Ready_to_be_garbage_collected:\n" +
                        "Active:");
        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldRunFilesStatusReportWithCustomMaxGC() throws Exception {
        // Given
        setStateStoreForTable("test-table", stateStore);
        in.enterNextPrompts(FILES_STATUS_REPORT_OPTION,
                "test-table", "1", "y", EXIT_OPTION);

        // When/Then
        String output = runClientGetOutput();
        assertThat(output).startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE + TABLE_SELECT_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .doesNotContain("defaulting to 1000")
                .contains("" +
                        "Files Status Report:\n" +
                        "--------------------------\n" +
                        "There are 2 leaf partitions and 1 non-leaf partitions")
                .contains("" +
                        "Ready_to_be_garbage_collected:\n" +
                        "Active:");
        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldReturnToMenuWhenOnTableNameScreen() throws Exception {
        // Given
        in.enterNextPrompts(FILES_STATUS_REPORT_OPTION, RETURN_TO_MAIN_SCREEN_OPTION, EXIT_OPTION);

        // When/Then
        String output = runClientGetOutput();
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE +
                TABLE_SELECT_SCREEN + CLEAR_CONSOLE + MAIN_SCREEN);
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(3)).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    private void confirmAndVerifyNoMoreInteractions() {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(4)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
