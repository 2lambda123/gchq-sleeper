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

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.FixedStateStoreProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createInstanceProperties;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createTableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;

public class CreateJobsTest {


    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);
    private final StateStore stateStore = inMemoryStateStoreWithNoPartitions();
    private final CompactionJobStatusStore jobStatusStore = new CompactionJobStatusStoreInMemory();

    @Test
    public void shouldCompactAllFilesInSinglePartition() throws Exception {
        // Given
        setPartitions(new PartitionsBuilder(schema).singlePartition("root").buildList());
        FileInfoFactory fileInfoFactory = fileInfoFactory();
        FileReference fileReference1 = fileInfoFactory.rootFile("file1", 200L);
        FileReference fileReference2 = fileInfoFactory.rootFile("file2", 200L);
        FileReference fileReference3 = fileInfoFactory.rootFile("file3", 200L);
        FileReference fileReference4 = fileInfoFactory.rootFile("file4", 200L);
        List<FileReference> files = List.of(fileReference1, fileReference2, fileReference3, fileReference4);
        setActiveFiles(files);

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).singleElement().satisfies(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2", "file3", "file4"))
                    .outputFile(job.getOutputFile())
                    .partitionId("root")
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), files);
            verifyJobCreationReported(job);
        });
    }

    @Test
    public void shouldCompactFilesInDifferentPartitions() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ddd")
                .buildList();
        setPartitions(partitions);
        FileInfoFactory fileInfoFactory = fileInfoFactory();
        FileReference fileReference1 = fileInfoFactory.partitionFile("B", "file1", 200L);
        FileReference fileReference2 = fileInfoFactory.partitionFile("B", "file2", 200L);
        FileReference fileReference3 = fileInfoFactory.partitionFile("C", "file3", 200L);
        FileReference fileReference4 = fileInfoFactory.partitionFile("C", "file4", 200L);
        setActiveFiles(List.of(fileReference1, fileReference2, fileReference3, fileReference4));

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).satisfiesExactlyInAnyOrder(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2"))
                    .outputFile(job.getOutputFile())
                    .partitionId("B")
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReference1, fileReference2));
            verifyJobCreationReported(job);
        }, job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file3", "file4"))
                    .outputFile(job.getOutputFile())
                    .partitionId("C")
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReference3, fileReference4));
            verifyJobCreationReported(job);
        });
    }

    @Test
    public void shouldCreateSplittingCompaction() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ddd")
                .buildList();
        setPartitions(partitions);
        FileInfoFactory fileInfoFactory = fileInfoFactory();
        FileReference fileReference1 = fileInfoFactory.partitionFile("A", "file1", 200L);
        FileReference fileReference2 = fileInfoFactory.partitionFile("A", "file2", 200L);
        setActiveFiles(List.of(fileReference1, fileReference2));

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).singleElement().satisfies(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2"))
                    .partitionId("A")
                    .isSplittingJob(true)
                    .childPartitions(List.of("B", "C"))
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReference1, fileReference2));
            verifyJobCreationReported(job);
        });
    }

    @Test
    public void shouldCreateStandardCompactionsToConvertSplitFilesToWholeFiles() throws Exception {
        // Given
        tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "1");
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ddd")
                .buildList();
        setPartitions(partitions);
        FileInfoFactory fileInfoFactory = fileInfoFactory();
        FileReference fileReference = fileInfoFactory.partitionFile("A", "file", 200L);
        FileReference fileReferenceLeft = SplitFileReference.referenceForChildPartition(fileReference, "B");
        FileReference fileReferenceRight = SplitFileReference.referenceForChildPartition(fileReference, "C");
        setActiveFiles(List.of(fileReferenceLeft, fileReferenceRight));

        // When
        List<CompactionJob> jobs = createJobs();

        // Then
        assertThat(jobs).satisfiesExactlyInAnyOrder(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of(fileReferenceLeft.getFilename()))
                    .outputFile(job.getOutputFile())
                    .partitionId("B")
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReferenceLeft));
            verifyJobCreationReported(job);
        }, job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of(fileReferenceRight.getFilename()))
                    .outputFile(job.getOutputFile())
                    .partitionId("C")
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReferenceRight));
            verifyJobCreationReported(job);
        });
    }

    @Test
    void shouldCreateJobsWhenStrategyDoesNotCreateJobsForWholeFilesWithForceCreateJobsFlagSet() throws Exception {
        // Given we use the BasicCompactionStrategy with a batch size of 3
        tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "3");
        setPartitions(new PartitionsBuilder(schema).singlePartition("root").buildList());
        FileInfoFactory fileInfoFactory = fileInfoFactory();
        // And we have 2 active whole files in the state store (which the BasicCompactionStrategy will skip
        // as it does not create jobs with fewer files than the batch size)
        FileReference fileReference1 = fileInfoFactory.rootFile("file1", 200L);
        FileReference fileReference2 = fileInfoFactory.rootFile("file2", 200L);
        List<FileReference> files = List.of(fileReference1, fileReference2);
        setActiveFiles(files);

        // When we force create jobs
        List<CompactionJob> jobs = forceCreateJobs();

        // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
        assertThat(jobs).satisfiesExactly(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1", "file2"))
                    .outputFile(job.getOutputFile())
                    .partitionId("root")
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReference1, fileReference2));
            verifyJobCreationReported(job);
        });
    }

    @Test
    void shouldCreateJobsWhenStrategyDoesNotCreateJobsForSplitFilesWithForceCreateJobsFlagSet() throws Exception {
        // Given we use the BasicCompactionStrategy with a batch size of 3
        tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "3");
        setPartitions(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .buildList());
        FileInfoFactory fileInfoFactory = fileInfoFactory();
        // And we have 1 active file that has been split in the state store (which the BasicCompactionStrategy
        // will skip as it does not create jobs with fewer files than the batch size)
        FileReference rootFile = fileInfoFactory.rootFile("file1", 2L);
        FileReference fileReference1 = SplitFileReference.referenceForChildPartition(rootFile, "L");
        List<FileReference> files = List.of(fileReference1);
        setActiveFiles(files);

        // When we force create jobs
        List<CompactionJob> jobs = forceCreateJobs();

        // Then a compaction job will be created for the files skipped by the BasicCompactionStrategy
        assertThat(jobs).satisfiesExactly(job -> {
            assertThat(job).isEqualTo(CompactionJob.builder()
                    .jobId(job.getId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .inputFiles(List.of("file1"))
                    .outputFile(job.getOutputFile())
                    .partitionId("L")
                    .isSplittingJob(false)
                    .build());
            verifySetJobForFilesInStateStore(job.getId(), List.of(fileReference1));
            verifyJobCreationReported(job);
        });
    }

    private FileInfoFactory fileInfoFactory() {
        return FileInfoFactory.from(schema, stateStore);
    }

    private void setPartitions(List<Partition> partitions) throws Exception {
        stateStore.initialise(partitions);
    }

    private void setActiveFiles(List<FileReference> files) throws Exception {
        stateStore.addFiles(files);
    }

    private void verifySetJobForFilesInStateStore(String jobId, List<FileReference> files) {
        assertThat(files).allSatisfy(file ->
                assertThat(getActiveStateFromStateStore(file).getJobId()).isEqualTo(jobId));
    }

    private FileReference getActiveStateFromStateStore(FileReference file) throws Exception {
        List<FileReference> foundRecords = stateStore.getActiveFiles().stream()
                .filter(found -> found.getPartitionId().equals(file.getPartitionId()))
                .filter(found -> found.getFilename().equals(file.getFilename()))
                .collect(Collectors.toUnmodifiableList());
        if (foundRecords.size() != 1) {
            throw new IllegalStateException("Expected one matching active file, found: " + foundRecords);
        }
        return foundRecords.get(0);
    }

    private void verifyJobCreationReported(CompactionJob job) {
        assertThat(jobStatusStore.getJob(job.getId()).orElseThrow())
                .usingRecursiveComparison().ignoringFields("createdStatus.updateTime")
                .isEqualTo(jobCreated(job, Instant.MAX));
    }

    private List<CompactionJob> createJobs() throws Exception {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        CreateJobs createJobs = CreateJobs.standard(ObjectFactory.noUserJars(), instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                compactionJobs::add, jobStatusStore);
        createJobs.createJobs();
        return compactionJobs;
    }

    private List<CompactionJob> forceCreateJobs() throws Exception {
        List<CompactionJob> compactionJobs = new ArrayList<>();
        CreateJobs createJobs = CreateJobs.compactAllFiles(ObjectFactory.noUserJars(), instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                compactionJobs::add, jobStatusStore);
        createJobs.createJobs();
        return compactionJobs;
    }
}
