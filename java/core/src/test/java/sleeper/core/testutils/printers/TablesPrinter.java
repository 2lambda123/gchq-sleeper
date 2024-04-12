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

package sleeper.core.testutils.printers;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A utility class to print the frequency of printed lines generated from a table name.
 */
public class TablesPrinter {

    private TablesPrinter() {
    }

    /**
     * Prints the frequency of lines generated using a table name. New lines are appended with information about how
     * frequent the generated lines are among other tables.
     *
     * @param  tableNames   the collection of table names
     * @param  tablePrinter the function that generates a printed line from a table name
     * @return              a string containing the generated lines, and information about how frequent the generated
     *                      lines are among tables.
     */
    public static String printForAllTables(Collection<String> tableNames, Function<String, String> tablePrinter) {
        Map<String, List<String>> tableNamesByPrintedValue = tableNames.stream()
                .collect(Collectors.groupingBy(tablePrinter));
        List<Map.Entry<String, List<String>>> printedLinesSortedByFrequency = tableNamesByPrintedValue.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getValue().size()))
                .collect(Collectors.toUnmodifiableList());
        ToStringPrintStream printer = new ToStringPrintStream();
        PrintStream out = printer.getPrintStream();

        for (Map.Entry<String, List<String>> entry : printedLinesSortedByFrequency) {
            String printedLine = entry.getKey();
            List<String> tablesThatPrintThisLine = entry.getValue();
            int frequency = tablesThatPrintThisLine.size();
            if (frequency == 1) {
                if (printedLinesSortedByFrequency.size() == 1) {
                    out.println("One table");
                } else {
                    out.println("Different for one table");
                }
            } else {
                out.println("Same for " + frequency + " tables");
            }
            out.println(printedLine);
        }
        return printer.toString();
    }
}
