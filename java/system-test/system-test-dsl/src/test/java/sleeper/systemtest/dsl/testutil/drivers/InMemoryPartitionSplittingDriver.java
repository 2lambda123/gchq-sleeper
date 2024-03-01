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
package sleeper.systemtest.dsl.testutil.drivers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.splitter.FindPartitionsToSplit;
import sleeper.splitter.FindPartitionsToSplit.JobSender;
import sleeper.splitter.SplitPartition;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.partitioning.PartitionSplittingDriver;

import java.io.IOException;

public class InMemoryPartitionSplittingDriver implements PartitionSplittingDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(InMemoryPartitionSplittingDriver.class);
    private final SystemTestInstanceContext instance;
    private final InMemorySketchesStore sketches;

    public InMemoryPartitionSplittingDriver(SystemTestInstanceContext instance, InMemorySketchesStore sketches) {
        this.instance = instance;
        this.sketches = sketches;
    }

    @Override
    public void splitPartitions() {
        new FindPartitionsToSplit(
                instance.getInstanceProperties(),
                instance.getTablePropertiesProvider(),
                instance.getStateStoreProvider(),
                splitPartition()).run();
    }

    private JobSender splitPartition() {
        return job -> {
            TableProperties tableProperties = instance.getTablePropertiesProvider().getById(job.getTableId());
            StateStore stateStore = instance.getStateStoreProvider().getStateStore(tableProperties);
            SplitPartition splitPartition = new SplitPartition(stateStore, tableProperties.getSchema(), sketches::load);
            try {
                splitPartition.splitPartition(job.getPartition(), job.getFileNames());
            } catch (IOException | StateStoreException e) {
                throw new RuntimeException("Failed to split partition", e);
            }
        };
    }
}
