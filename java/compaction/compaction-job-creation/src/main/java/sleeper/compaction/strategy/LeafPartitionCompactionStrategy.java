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
package sleeper.compaction.strategy;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileInfo;

import java.util.List;

public interface LeafPartitionCompactionStrategy {
    default void init(InstanceProperties instanceProperties, TableProperties tableProperties, CompactionJobFactory factory, boolean createJobIfBatchSizeNotMet) {
        init(instanceProperties, tableProperties, factory);
    }

    void init(InstanceProperties instanceProperties, TableProperties tableProperties, CompactionJobFactory factory);

    List<CompactionJob> createJobsForLeafPartition(Partition partition, List<FileInfo> activeFilesWithNoJobId);
}
