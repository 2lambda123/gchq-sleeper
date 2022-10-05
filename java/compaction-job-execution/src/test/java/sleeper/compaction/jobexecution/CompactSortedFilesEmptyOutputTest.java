/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.jobexecution;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestBase;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestDataHelper;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.statestore.StateStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.combineSortedBySingleKey;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.assertReadyForGC;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createInitStateStore;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createStateStore;

public class CompactSortedFilesEmptyOutputTest extends CompactSortedFilesTestBase {

    @Test
    public void filesShouldMergeCorrectlyWhenSomeAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createInitStateStore(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data = keyAndTwoValuesSortedEvenLongs();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", Collections.emptyList(), null, null);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getLinesRead()).isEqualTo(data.size());
        assertThat(summary.getLinesWritten()).isEqualTo(data.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 100L, 0L, 198L));
    }

    @Test
    public void filesShouldMergeCorrectlyWhenAllAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createInitStateStore(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        dataHelper.writeLeafFile(folderName + "/file1.parquet", Collections.emptyList(), null, null);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", Collections.emptyList(), null, null);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getLinesRead()).isZero();
        assertThat(summary.getLinesWritten()).isZero();
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEmpty();

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 0L, null, null));
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWhenOneChildFileIsEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createStateStore(schema);
        stateStore.initialise(new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(200L))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "C", "A", "B", 200L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getLinesRead()).isEqualTo(200L);
        assertThat(summary.getLinesWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(expectedResults);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEmpty();

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 200L, 0L, 199L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 0L, null, null));
    }
}
