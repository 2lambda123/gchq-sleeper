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
package sleeper.compaction.job.creation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.strategy.CompactionStrategy;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.SplitFileReferences;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * Creates compaction job definitions and posts them to an SQS queue.
 * <p>
 * This is done as follows:
 * - Queries the {@link StateStore} for active files which do not have a job id.
 * - Groups these by partition.
 * - For each partition, uses the configurable {@link CompactionStrategy} to
 * decide what compaction jobs to create.
 * - These compaction jobs are then sent to SQS.
 */
public class CreateCompactionJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateCompactionJobs.class);

    private final ObjectFactory objectFactory;
    private final InstanceProperties instanceProperties;
    private final JobSender jobSender;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore jobStatusStore;
    private final boolean compactAllFiles;

    private CreateCompactionJobs(ObjectFactory objectFactory,
                                 InstanceProperties instanceProperties,
                                 TablePropertiesProvider tablePropertiesProvider,
                                 StateStoreProvider stateStoreProvider,
                                 JobSender jobSender,
                                 CompactionJobStatusStore jobStatusStore,
                                 boolean compactAllFiles) {
        this.objectFactory = objectFactory;
        this.instanceProperties = instanceProperties;
        this.jobSender = jobSender;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.jobStatusStore = jobStatusStore;
        this.compactAllFiles = compactAllFiles;
    }

    public static CreateCompactionJobs compactAllFiles(ObjectFactory objectFactory,
                                                       InstanceProperties instanceProperties,
                                                       TablePropertiesProvider tablePropertiesProvider,
                                                       StateStoreProvider stateStoreProvider,
                                                       JobSender jobSender,
                                                       CompactionJobStatusStore jobStatusStore) {
        return new CreateCompactionJobs(objectFactory, instanceProperties, tablePropertiesProvider, stateStoreProvider, jobSender, jobStatusStore, true);
    }

    public static CreateCompactionJobs standard(ObjectFactory objectFactory,
                                                InstanceProperties instanceProperties,
                                                TablePropertiesProvider tablePropertiesProvider,
                                                StateStoreProvider stateStoreProvider,
                                                JobSender jobSender,
                                                CompactionJobStatusStore jobStatusStore) {
        return new CreateCompactionJobs(objectFactory, instanceProperties, tablePropertiesProvider, stateStoreProvider, jobSender, jobStatusStore, false);
    }

    public void createJobs() throws StateStoreException, IOException, ObjectFactoryException {
        List<TableProperties> tables = tablePropertiesProvider.streamOnlineTables()
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Found {} online tables", tables.size());
        for (TableProperties table : tables) {
            createJobs(table);
        }
    }

    public void createJobs(TableProperties table) throws StateStoreException, IOException, ObjectFactoryException {
        LOGGER.info("Performing pre-splits on files in {}", table.getId());
        SplitFileReferences.from(stateStoreProvider.getStateStore(table)).split();
        createJobsForTable(table);
    }

    private void createJobsForTable(TableProperties tableProperties) throws StateStoreException, IOException, ObjectFactoryException {
        TableStatus tableId = tableProperties.getId();
        LOGGER.debug("Creating jobs for table {}", tableId);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);

        List<Partition> allPartitions = stateStore.getAllPartitions();

        List<FileReference> fileReferences = stateStore.getFileReferences();
        // NB We retrieve the information about all the active file references and filter
        // that, rather than making separate calls to the state store for reasons
        // of efficiency and to ensure consistency.
        List<FileReference> fileReferencesWithNoJobId = fileReferences.stream().filter(f -> null == f.getJobId()).collect(Collectors.toList());
        List<FileReference> fileReferencesWithJobId = fileReferences.stream().filter(f -> null != f.getJobId()).collect(Collectors.toList());
        LOGGER.debug("Found {} file references with no job id in table {}", fileReferencesWithNoJobId.size(), tableId);
        LOGGER.debug("Found {} file references with a job id in table {}", fileReferencesWithJobId.size(), tableId);

        CompactionStrategy compactionStrategy = objectFactory
                .getObject(tableProperties.get(COMPACTION_STRATEGY_CLASS), CompactionStrategy.class);
        LOGGER.debug("Created compaction strategy of class {}", tableProperties.get(COMPACTION_STRATEGY_CLASS));
        compactionStrategy.init(instanceProperties, tableProperties);

        List<CompactionJob> compactionJobs = compactionStrategy.createCompactionJobs(fileReferencesWithJobId, fileReferencesWithNoJobId, allPartitions);
        LOGGER.info("Used {} to create {} compaction jobs for table {}", compactionStrategy.getClass().getSimpleName(), compactionJobs.size(), tableId);

        if (compactAllFiles) {
            createJobsFromLeftoverFiles(tableProperties, fileReferencesWithNoJobId, allPartitions, compactionJobs);
        }
        for (CompactionJob compactionJob : compactionJobs) {
            // Record job was created before we send it to SQS, otherwise this update can conflict with a compaction
            // task trying to record that the job was started.
            jobStatusStore.jobCreated(compactionJob);

            // Send compaction job to SQS (NB Send compaction job to SQS before updating the job field of the files in the
            // StateStore so that if the send to SQS fails then the StateStore will not be updated and later another
            // job can be created for these files.)
            jobSender.send(compactionJob);

            // Update the statuses of these files to record that a compaction job is in progress
            LOGGER.debug("Updating status of files in StateStore");
            List<FileReference> fileReferences1 = new ArrayList<>();
            for (String filename : compactionJob.getInputFiles()) {
                for (FileReference fileReference : fileReferences) {
                    if (fileReference.getPartitionId().equals(compactionJob.getPartitionId())
                            && fileReference.getFilename().equals(filename)) {
                        fileReferences1.add(fileReference);
                        break;
                    }
                }
            }
            stateStore.atomicallyAssignJobIdToFileReferences(compactionJob.getId(), fileReferences1);
        }
    }

    private void createJobsFromLeftoverFiles(TableProperties tableProperties, List<FileReference> activeFileReferencesWithNoJobId,
                                             List<Partition> allPartitions, List<CompactionJob> compactionJobs) {
        LOGGER.info("Creating compaction jobs for all files");
        int jobsBefore = compactionJobs.size();
        int batchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
        Set<String> leafPartitionIds = allPartitions.stream()
                .filter(Partition::isLeafPartition)
                .map(Partition::getId)
                .collect(Collectors.toSet());
        Set<String> assignedFiles = compactionJobs.stream()
                .flatMap(job -> job.getInputFiles().stream())
                .collect(Collectors.toSet());
        List<FileReference> leftoverFiles = activeFileReferencesWithNoJobId.stream()
                .filter(file -> !assignedFiles.contains(file.getFilename()))
                .collect(Collectors.toList());
        Map<String, List<FileReference>> filesByPartitionId = new HashMap<>();
        leftoverFiles.stream()
                .filter(fileReference -> leafPartitionIds.contains(fileReference.getPartitionId()))
                .forEach(fileReference -> filesByPartitionId.computeIfAbsent(fileReference.getPartitionId(),
                        (key) -> new ArrayList<>()).add(fileReference));
        CompactionJobFactory factory = new CompactionJobFactory(instanceProperties, tableProperties);
        for (Map.Entry<String, List<FileReference>> fileByPartitionId : filesByPartitionId.entrySet()) {
            List<FileReference> filesForJob = new ArrayList<>();
            for (FileReference fileReference : fileByPartitionId.getValue()) {
                filesForJob.add(fileReference);
                if (filesForJob.size() >= batchSize) {
                    LOGGER.info("Creating a job to compact {} files in partition {} in table {}",
                            filesForJob.size(), fileByPartitionId.getKey(), tableProperties.get(TABLE_NAME));
                    compactionJobs.add(factory.createCompactionJob(filesForJob, fileByPartitionId.getKey()));
                    filesForJob.clear();
                }
            }
            if (!filesForJob.isEmpty()) {
                LOGGER.info("Creating a job to compact {} files in partition {} in table {}",
                        filesForJob.size(), fileByPartitionId.getKey(), tableProperties.get(TABLE_NAME));
                compactionJobs.add(factory.createCompactionJob(filesForJob, fileByPartitionId.getKey()));
            }
        }
        LOGGER.info("Created {} jobs from {} leftover files for table {}",
                compactionJobs.size() - jobsBefore, leftoverFiles.size(), tableProperties.getId());
    }

    @FunctionalInterface
    public interface JobSender {
        void send(CompactionJob compactionJob) throws IOException;
    }
}
