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
package sleeper.compaction.strategy.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.strategy.ShouldCreateJobsStrategy;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.FileReference;

import java.util.List;
import java.util.stream.Collectors;

public class SizeRatioShouldCreateJobsStrategy implements ShouldCreateJobsStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(SizeRatioShouldCreateJobsStrategy.class);

    private long maxConcurrentCompactionJobsPerPartition;

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties) {
        maxConcurrentCompactionJobsPerPartition = tableProperties.getLong(TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION);
    }

    @Override
    public long maxCompactionJobsToCreate(String partitionId, List<FileReference> activeFilesWithJobId) {
        long numConcurrentCompactionJobs = getNumberOfCurrentCompactionJobs(activeFilesWithJobId);
        if (numConcurrentCompactionJobs >= maxConcurrentCompactionJobsPerPartition) {
            LOGGER.info("Not creating compaction jobs for partition {} as there are already {} running compaction jobs",
                    partitionId, numConcurrentCompactionJobs);
            return 0;
        }
        long maxNumberOfJobsToCreate = maxConcurrentCompactionJobsPerPartition - numConcurrentCompactionJobs;
        LOGGER.info("Max jobs to create = {}", maxNumberOfJobsToCreate);
        return maxNumberOfJobsToCreate;
    }

    private long getNumberOfCurrentCompactionJobs(List<FileReference> activeFilesWithJobId) {
        return activeFilesWithJobId.stream()
                .map(FileReference::getJobId)
                .collect(Collectors.toSet())
                .size();
    }
}
