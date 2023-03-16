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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.menu.Chosen;
import sleeper.console.menu.ConsoleChoice;
import sleeper.statestore.StateStoreException;
import sleeper.status.report.FilesStatusReport;
import sleeper.status.report.filestatus.StandardFileStatusReporter;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class FilesStatusReportScreen {
    //<instance id> <table name> <optional_max_num_ready_for_gc_files_to_count> <optional_verbose_true_or_false>
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final AdminConfigStore store;
    private final TableSelectHelper tableSelectHelper;

    public FilesStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.in = in;
        this.store = store;
        this.tableSelectHelper = new TableSelectHelper(out, in);
    }

    public void chooseTableAndPrint(String instanceId) {
        Chosen<ConsoleChoice> chosen = tableSelectHelper.chooseTable();
        if (chosen.getChoice().isEmpty()) {
            String tableName = chosen.getEntered();
            TableProperties tableProperties = store.loadTableProperties(instanceId, tableName);
            if (tableProperties == null) {
                out.println();
                out.printf("Error: Properties for table \"%s\" could not be found\n", tableName);
            } else {
                chooseOptionalArgsAndPrint(instanceId, tableName);
            }
        }
        confirmReturnToMainScreen(out, in);
    }

    public void chooseOptionalArgsAndPrint(String instanceId, String tableName) {
        int maxReadyForGCFiles = 1000;
        String maxGcArg = in.promptLine("Enter the number for maxReadyForGCFiles (default is " + maxReadyForGCFiles + "): ");
        if (maxGcArg.isEmpty()) {
            out.println("No value entered, defaulting to " + maxReadyForGCFiles);
        } else {
            try {
                maxReadyForGCFiles = Integer.parseInt(maxGcArg);
            } catch (NumberFormatException e) {
                out.println("Failed to convert maxReadyForGCFiles to integer, defaulting to " + maxReadyForGCFiles);
            }
        }
        boolean verbose = in.promptLine("Run report in verbose mode? (y/N): ").equalsIgnoreCase("y");
        try {
            new FilesStatusReport(store.loadStateStore(instanceId, tableName), maxReadyForGCFiles, verbose,
                    new StandardFileStatusReporter(out.printStream())).run();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
