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

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableStatus;

import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A utility class to generate strings using partitions in tables. Intended to be used for approval tests.
 * Uses {@link TablesPrinter} to consolidate the generated output for multiple tables.
 */
public class PartitionsPrinter {

    private PartitionsPrinter() {
    }

    /**
     * Generates a string with information about partitions for all provided tables. The tables must have
     * matching {@link Schema}s in order to deserialise the partition regions correctly.
     *
     * @param  schema the schema for all tables
     * @param  tables the list of tables
     * @param  tree   the expected {@link PartitionTree}
     * @return        a generated string
     */
    public static String printExpectedPartitionsForAllTables(Schema schema, List<TableStatus> tables, PartitionTree tree) {
        return printExpectedPartitionsForAllTables(schema,
                tables.stream().map(TableStatus::getTableName).collect(Collectors.toUnmodifiableList()), tree);
    }

    /**
     * Generates a string with information about partitions for all provided tables. The tables must have
     * matching {@link Schema}s in order to deserialise the partition regions correctly.
     *
     * @param  schema     the schema for all tables
     * @param  tableNames the collection of table names
     * @param  tree       the expected {@link PartitionTree}
     * @return            a generated string
     */
    public static String printExpectedPartitionsForAllTables(Schema schema, Collection<String> tableNames, PartitionTree tree) {
        return printTablePartitionsExpectingIdentical(schema, tableNames.stream()
                .collect(Collectors.toMap(table -> table, table -> tree)));
    }

    /**
     * Generates a string with information about partitions for all provided tables. The tables must have
     * matching {@link Schema}s in order to deserialise the partition regions correctly.
     *
     * @param  schema            the schema for all tables
     * @param  partitionsByTable the map of table name to {@link PartitionTree}
     * @return                   a generated string
     */
    public static String printTablePartitionsExpectingIdentical(Schema schema, Map<String, PartitionTree> partitionsByTable) {
        return TablesPrinter.printForAllTablesExcludingNames(partitionsByTable.keySet(), table -> printPartitions(schema, partitionsByTable.get(table)));
    }

    /**
     * Generates a string with information about partitions.
     *
     * @param  schema        the schema for all tables
     * @param  partitionTree the {@link PartitionTree}
     * @return               a generated string
     */
    public static String printPartitions(Schema schema, PartitionTree partitionTree) {
        ToStringPrintWriter printer = new ToStringPrintWriter();
        PrintStream out = printer.getPrintStream();
        RegionSerDe regionSerDe = new RegionSerDe(schema);
        partitionTree.traverseLeavesFirst().forEach(partition -> {
            String locationName = buildPartitionLocationName(partition, partitionTree);
            out.println("Partition at " + locationName + ":");
            out.println(regionSerDe.toJson(partition.getRegion()));
        });
        return printer.toString();
    }

    static String buildPartitionLocationName(Partition partition, PartitionTree tree) {
        String parentId = partition.getParentPartitionId();
        if (parentId == null) {
            return "root";
        }
        String partitionId = partition.getId();
        StringBuilder name = new StringBuilder();
        while (parentId != null) {
            Partition parent = tree.getPartition(parentId);
            name.append(getPartitionSideOfParentName(partitionId, parent));
            partitionId = parent.getId();
            parentId = parent.getParentPartitionId();
        }
        return name.reverse().toString();
    }

    private static char getPartitionSideOfParentName(String partitionId, Partition parent) {
        int index = parent.getChildPartitionIds().indexOf(partitionId);
        if (index == 0) {
            return 'L';
        } else if (index == 1) {
            return 'R';
        } else {
            throw new IllegalStateException("Unexpected index " + index + " for partition " + partitionId + " in parent: " + parent);
        }
    }
}
