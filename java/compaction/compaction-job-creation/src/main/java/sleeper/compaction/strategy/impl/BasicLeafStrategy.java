/*
 * Copyright 2023 Crown Copyright
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
package sleeper.compaction.strategy.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.strategy.LeafPartitionCompactionStrategy;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.List;

import static sleeper.compaction.strategy.impl.CompactionUtils.getFilesInAscendingOrder;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;

public class BasicLeafStrategy implements LeafPartitionCompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicLeafStrategy.class);

    private CompactionJobFactory factory;
    private int compactionFilesBatchSize;

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties, CompactionJobFactory factory) {
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
        this.factory = factory;
    }

    @Override
    public List<CompactionJob> createJobsForLeafPartition(Partition partition, List<FileInfo> fileInfos) {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        List<FileInfo> filesInAscendingOrder = getFilesInAscendingOrder(partition, fileInfos);

        // Iterate through files, creating jobs for batches of maximumNumberOfFilesToCompact files
        List<FileInfo> filesForJob = new ArrayList<>();
        for (FileInfo fileInfo : filesInAscendingOrder) {
            filesForJob.add(fileInfo);
            if (filesForJob.size() >= compactionFilesBatchSize) {
                // Create job for these files
                LOGGER.info("Creating a job to compact {} files in partition {}",
                        filesForJob.size(), partition);
                compactionJobs.add(factory.createCompactionJob(filesForJob, partition.getId()));
                filesForJob.clear();
            }
        }
        return compactionJobs;
    }
}
