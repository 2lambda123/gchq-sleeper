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

package sleeper.systemtest.dsl.partitioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.PollWithRetries;
import sleeper.splitter.FindPartitionToSplitResult;
import sleeper.splitter.FindPartitionsToSplit;
import sleeper.statestore.StateStoreProvider;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class WaitForPartitionSplitting {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForPartitionSplitting.class);

    private static final PollWithRetries WAIT_FOR_SPLITS = PollWithRetries
            .intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(1));
    private final Map<String, Set<String>> partitionIdsByTableId;

    private WaitForPartitionSplitting(List<FindPartitionToSplitResult> toSplit) {
        partitionIdsByTableId = toSplit.stream()
                .collect(Collectors.groupingBy(FindPartitionToSplitResult::getTableId,
                        Collectors.mapping(split -> split.getPartition().getId(), Collectors.toSet())));
    }

    public static WaitForPartitionSplitting forCurrentPartitionsNeedingSplitting(
            TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) {
        return new WaitForPartitionSplitting(getResults(propertiesProvider, stateStoreProvider));
    }

    public void pollUntilFinished(TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) throws InterruptedException {
        LOGGER.info("Waiting for splits, expecting partitions to be split: {}", partitionIdsByTableId);
        WAIT_FOR_SPLITS.pollUntil("partition splits finished",
                () -> new FinishedCheck(propertiesProvider, stateStoreProvider).isFinished());
    }

    public boolean isSplitFinished(TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) {
        return new FinishedCheck(propertiesProvider, stateStoreProvider).isFinished();
    }

    private class FinishedCheck {
        private final TablePropertiesProvider propertiesProvider;
        private final StateStoreProvider stateStoreProvider;

        FinishedCheck(TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) {
            this.propertiesProvider = propertiesProvider;
            this.stateStoreProvider = stateStoreProvider;
        }

        public boolean isFinished() {
            return partitionIdsByTableId.keySet().stream().parallel()
                    .allMatch(this::isTableFinished);
        }

        public boolean isTableFinished(String tableId) {
            TableProperties properties = propertiesProvider.getById(tableId);
            StateStore stateStore = stateStoreProvider.getStateStore(properties);
            Set<String> leafPartitionIds = getLeafPartitionIds(stateStore);
            List<String> unsplit = partitionIdsByTableId.get(tableId).stream()
                    .filter(leafPartitionIds::contains)
                    .collect(Collectors.toUnmodifiableList());
            LOGGER.info("Found unsplit partitions in table {}: {}", properties.getStatus(), unsplit);
            return unsplit.isEmpty();
        }
    }

    private static Set<String> getLeafPartitionIds(StateStore stateStore) {
        try {
            return stateStore.getLeafPartitions().stream()
                    .map(Partition::getId)
                    .collect(Collectors.toSet());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<FindPartitionToSplitResult> getResults(
            TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) {

        // Collect all table properties and state stores first to avoid concurrency problems with providers
        List<TableProperties> tableProperties = propertiesProvider.streamOnlineTables()
                .collect(Collectors.toUnmodifiableList());
        Map<String, StateStore> stateStoreByTableId = tableProperties.stream()
                .map(properties -> entry(properties.get(TABLE_ID), stateStoreProvider.getStateStore(properties)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        return tableProperties.stream().parallel()
                .flatMap(properties -> {
                    try {
                        return FindPartitionsToSplit.getResults(properties, stateStoreByTableId.get(properties.get(TABLE_ID))).stream();
                    } catch (StateStoreException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toUnmodifiableList());
    }
}
