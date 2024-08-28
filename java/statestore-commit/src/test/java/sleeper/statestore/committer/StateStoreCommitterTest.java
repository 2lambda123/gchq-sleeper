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
package sleeper.statestore.committer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.statestore.FixedStateStoreProvider;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.FilesReportTestHelper;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequest;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.statestore.committer.StateStoreCommitter.RequestHandle;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFailedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.compaction.job.status.CompactionJobStartedEvent.compactionJobStarted;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFiles;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithPartitions;
import static sleeper.ingest.job.status.IngestJobStartedEvent.ingestJobStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestAddedFilesStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestStartedStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;

public class StateStoreCommitterTest {
    private static final Instant DEFAULT_FILE_UPDATE_TIME = FilesReportTestHelper.DEFAULT_UPDATE_TIME;
    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, DEFAULT_FILE_UPDATE_TIME);
    private final InMemoryCompactionJobStatusStore compactionJobStatusStore = new InMemoryCompactionJobStatusStore();
    private final InMemoryIngestJobStatusStore ingestJobStatusStore = new InMemoryIngestJobStatusStore();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final List<TableProperties> tables = new ArrayList<>();
    private final Map<String, StateStore> stateStoreByTableId = new HashMap<>();
    private final List<StateStoreCommitRequest> failedRequests = new ArrayList<>();

    @Nested
    @DisplayName("Commit a compaction job")
    class CommitCompaction {

        @Test
        void shouldApplyCompactionCommitRequest() throws Exception {
            // Given
            StateStore stateStore = createTable("test-table");
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(2), 123, 123);
            Instant commitTime = Instant.parse("2024-06-14T15:40:00Z");
            CompactionJobCommitRequest request = createFinishedCompactionForTable("test-table", createdTime, startTime, summary);

            // When
            committerWithTimes(List.of(commitTime)).apply(StateStoreCommitRequest.forCompactionJob(request));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(
                    fileFactory.rootFile("output.parquet", 123L));
            assertThat(compactionJobStatusStore.getAllJobs("test-table")).containsExactly(
                    jobCreated(request.getJob(), createdTime,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .finishedStatus(compactionFinishedStatus(summary))
                                    .statusUpdate(compactionCommittedStatus(commitTime))
                                    .build()));
        }

        @Test
        void shouldFailWhenInputFileDoesNotExist() throws Exception {
            // Given
            StateStore stateStore = createTable("test-table");
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            Instant failedTime = Instant.parse("2024-06-14T15:38:00Z");
            RecordsProcessedSummary summary = summary(startTime, finishTime, 123, 123);
            CompactionJobCommitRequest request = createFinishedCompactionForTable("test-table", createdTime, startTime, summary);
            stateStore.clearFileData();

            // When
            StateStoreCommitter committer = committerWithTimes(List.of(failedTime));
            assertThatThrownBy(() -> committer.apply(StateStoreCommitRequest.forCompactionJob(request)))
                    .isInstanceOf(ReplaceRequestsFailedException.class)
                    .hasCauseInstanceOf(FileNotFoundException.class);

            // Then
            assertThat(stateStore.getFileReferences()).isEmpty();
            assertThat(compactionJobStatusStore.getAllJobs("test-table")).containsExactly(
                    jobCreated(request.getJob(), createdTime,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .statusUpdate(compactionFinishedStatus(summary))
                                    .finishedStatus(compactionFailedStatus(
                                            new ProcessRunTime(finishTime, failedTime),
                                            List.of("1 replace file reference requests failed to update the state store", "File not found: input.parquet")))
                                    .build()));
        }

        @Test
        void shouldFailWhenStateStoreThrowsUnexpectedRuntimeException() throws Exception {
            // Given
            StateStore stateStore = mock(StateStore.class);
            createTable("test-table", stateStore);
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            Instant failedTime = Instant.parse("2024-06-14T15:38:00Z");
            RecordsProcessedSummary summary = summary(startTime, finishTime, 123, 123);
            CompactionJobCommitRequest request = createFinishedCompactionForTable("test-table", createdTime, startTime, summary);
            RuntimeException failure = new RuntimeException("Unexpected failure");
            doThrow(failure).when(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(
                    CompactionJobCommitter.replaceFileReferencesRequest(request.getJob(), request.getRecordsWritten())));

            // When
            StateStoreCommitter committer = committerWithTimes(List.of(failedTime));
            assertThatThrownBy(() -> committer.apply(StateStoreCommitRequest.forCompactionJob(request)))
                    .isSameAs(failure);

            // Then
            assertThat(compactionJobStatusStore.getAllJobs("test-table")).containsExactly(
                    jobCreated(request.getJob(), createdTime,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .statusUpdate(compactionFinishedStatus(summary))
                                    .finishedStatus(compactionFailedStatus(
                                            new ProcessRunTime(finishTime, failedTime),
                                            List.of("Unexpected failure")))
                                    .build()));
        }

        @Test
        void shouldFailWhenStateStoreThrowsUnexpectedStateStoreException() throws Exception {
            // Given
            StateStore stateStore = mock(StateStore.class);
            createTable("test-table", stateStore);
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            Instant failedTime = Instant.parse("2024-06-14T15:38:00Z");
            RecordsProcessedSummary summary = summary(startTime, finishTime, 123, 123);
            CompactionJobCommitRequest request = createFinishedCompactionForTable("test-table", createdTime, startTime, summary);
            ReplaceFileReferencesRequest expectedRequest = CompactionJobCommitter.replaceFileReferencesRequest(request.getJob(), request.getRecordsWritten());
            RuntimeException cause = new RuntimeException("Unexpected failure");
            ReplaceRequestsFailedException failure = new ReplaceRequestsFailedException(List.of(expectedRequest), cause);
            doThrow(failure).when(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(expectedRequest));

            // When
            StateStoreCommitter committer = committerWithTimes(List.of(failedTime));
            assertThatThrownBy(() -> committer.apply(StateStoreCommitRequest.forCompactionJob(request)))
                    .isSameAs(failure);

            // Then
            assertThat(compactionJobStatusStore.getAllJobs("test-table")).containsExactly(
                    jobCreated(request.getJob(), createdTime,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .statusUpdate(compactionFinishedStatus(summary))
                                    .finishedStatus(compactionFailedStatus(
                                            new ProcessRunTime(finishTime, failedTime),
                                            List.of("1 replace file reference requests failed to update the state store", "Unexpected failure")))
                                    .build()));
        }
    }

    @Nested
    @DisplayName("Assign job ID to files")
    class AssignJobIdToFiles {
        @Test
        void shouldApplyCompactionJobIdAssignmentCommitRequest() throws Exception {
            // Given
            StateStore stateStore = createTable("test-table");
            FileReference inputFile = fileFactory.rootFile("input.parquet", 123L);
            stateStore.addFile(inputFile);

            // When
            CompactionJobIdAssignmentCommitRequest request = new CompactionJobIdAssignmentCommitRequest(List.of(
                    assignJobOnPartitionToFiles("job1", "root", List.of("input.parquet"))), "test-table");
            committer().apply(StateStoreCommitRequest.forCompactionJobIdAssignment(request));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(
                    withJobId("job1", fileFactory.rootFile("input.parquet", 123L)));
        }

        @Test
        void shouldFailToApplyJobIdAssignmentIfFileReferenceIsAlreadyAssignedToJob() throws Exception {
            // Given
            StateStore stateStore = createTable("test-table");
            FileReference inputFile = fileFactory.rootFile("input.parquet", 123L);
            stateStore.addFile(inputFile);
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("job1", "root", List.of("input.parquet"))));

            // When
            CompactionJobIdAssignmentCommitRequest request = new CompactionJobIdAssignmentCommitRequest(List.of(
                    assignJobOnPartitionToFiles("job2", "root", List.of("input.parquet"))), "test-table");
            assertThatThrownBy(() -> committer().apply(StateStoreCommitRequest.forCompactionJobIdAssignment(request)))
                    .isInstanceOf(FileReferenceAssignedToJobException.class);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(
                    withJobId("job1", fileFactory.rootFile("input.parquet", 123L)));
        }

        @Test
        void shouldFailToApplyJobIdAssignmentIfFileReferenceDoesNotExist() throws Exception {
            // Given
            StateStore stateStore = createTable("test-table");

            // When
            CompactionJobIdAssignmentCommitRequest request = new CompactionJobIdAssignmentCommitRequest(List.of(
                    assignJobOnPartitionToFiles("job2", "root", List.of("input.parquet"))), "test-table");
            assertThatThrownBy(() -> committer().apply(StateStoreCommitRequest.forCompactionJobIdAssignment(request)))
                    .isInstanceOf(FileReferenceNotFoundException.class);

            // Then
            assertThat(stateStore.getFileReferences()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Add files during ingest")
    class AddFiles {

        @Test
        void shouldApplyIngestJobAddFilesCommitRequest() throws Exception {
            // Given we have a commit request during an ingest job, which may still be in progress
            StateStore stateStore = createTable("test-table");
            FileReference outputFile = fileFactory.rootFile("output.parquet", 123L);
            IngestJob ingestJob = IngestJob.builder()
                    .id("test-job")
                    .tableId("test-table")
                    .files(List.of("input.parquet"))
                    .build();
            Instant startTime = Instant.parse("2024-06-20T14:50:00Z");
            Instant writtenTime = Instant.parse("2024-06-20T14:55:01Z");
            IngestAddFilesCommitRequest commitRequest = IngestAddFilesCommitRequest.builder()
                    .ingestJob(ingestJob)
                    .taskId("test-task-id")
                    .jobRunId("test-job-run-id")
                    .fileReferences(List.of(outputFile))
                    .writtenTime(writtenTime)
                    .build();

            ingestJobStatusStore.jobStarted(ingestJobStarted(ingestJob, startTime)
                    .taskId("test-task-id").jobRunId("test-job-run-id").build());

            // When
            committer().apply(StateStoreCommitRequest.forIngestAddFiles(commitRequest));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(outputFile);
            assertThat(ingestJobStatusStore.getAllJobs("test-table"))
                    .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                            .taskId("test-task-id")
                            .startedStatus(ingestStartedStatus(ingestJob, startTime))
                            .statusUpdate(ingestAddedFilesStatus(writtenTime, 1))
                            .build()));
        }

        @Test
        void shouldApplyIngestStreamAddFilesCommitRequest() throws Exception {
            // Given we have a commit request without an ingest job (e.g. from an endless stream of records)
            StateStore stateStore = createTable("test-table");
            FileReference outputFile = fileFactory.rootFile("output.parquet", 123L);
            IngestAddFilesCommitRequest commitRequest = IngestAddFilesCommitRequest.builder()
                    .tableId("test-table")
                    .fileReferences(List.of(outputFile))
                    .build();

            // When
            committer().apply(StateStoreCommitRequest.forIngestAddFiles(commitRequest));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(outputFile);
            assertThat(ingestJobStatusStore.getAllJobs("test-table")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Split a partition to create child partitions")
    class SplitPartition {

        @Test
        void shouldApplySplitPartitionRequest() throws Exception {
            // Given
            PartitionTree afterTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "aaa")
                    .buildTree();
            StateStore stateStore = createTable("test-table");
            SplitPartitionCommitRequest commitRequest = new SplitPartitionCommitRequest("test-table",
                    afterTree.getPartition("root"),
                    afterTree.getPartition("left"),
                    afterTree.getPartition("right"));

            // When
            committer().apply(StateStoreCommitRequest.forSplitPartition(commitRequest));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(afterTree.getAllPartitions());
        }

        @Test
        void shouldFailWhenPartitionHasAlreadyBeenSplit() throws Exception {
            // Given
            PartitionTree afterTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "aaa")
                    .buildTree();
            StateStore stateStore = createTable("test-table");
            stateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                    afterTree.getPartition("root"),
                    afterTree.getPartition("left"),
                    afterTree.getPartition("right"));
            SplitPartitionCommitRequest commitRequest = new SplitPartitionCommitRequest("test-table",
                    afterTree.getPartition("root"),
                    afterTree.getPartition("left"),
                    afterTree.getPartition("right"));

            // When / Then
            assertThatThrownBy(() -> committer().apply(StateStoreCommitRequest.forSplitPartition(commitRequest)))
                    .isInstanceOf(StateStoreException.class);
        }
    }

    @Nested
    @DisplayName("Commit when garbage collector deleted files")
    class CommitGC {

        @Test
        void shouldApplyDeletedFilesRequest() throws Exception {
            // Given two files have been replaced by a compaction job
            StateStore stateStore = createTable("test-table");
            stateStore.addFiles(List.of(
                    fileFactory.rootFile("file1.parquet", 100),
                    fileFactory.rootFile("file2.parquet", 200)));
            List<String> filenames = List.of("file1.parquet", "file2.parquet");
            stateStore.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("test-job", "root", filenames)));
            FileReference fileAfterCompaction = fileFactory.rootFile("after.parquet", 300);
            stateStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(
                    replaceJobFileReferences("test-job", "root", filenames, fileAfterCompaction)));
            // And we have a request to commit that they have been deleted
            GarbageCollectionCommitRequest request = new GarbageCollectionCommitRequest("test-table", filenames);

            // When
            committer().apply(StateStoreCommitRequest.forGarbageCollection(request));

            // Then
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(3))
                    .isEqualTo(activeAndReadyForGCFiles(List.of(fileAfterCompaction), List.of()));
        }

        @Test
        void shouldFailWhenFileStillHasAReference() throws Exception {
            // Given
            StateStore stateStore = createTable("test-table");
            stateStore.addFiles(List.of(fileFactory.rootFile("file.parquet", 100)));
            GarbageCollectionCommitRequest request = new GarbageCollectionCommitRequest(
                    "test-table", List.of("file.parquet"));

            // When / Then
            assertThatThrownBy(() -> committer().apply(StateStoreCommitRequest.forGarbageCollection(request)))
                    .isInstanceOf(StateStoreException.class);
        }
    }

    @Nested
    @DisplayName("Apply batches of requests")
    class BatchRequests {

        @Test
        void shouldApplyRequestAsBatch() throws Exception {
            // Given
            StateStore stateStore = createTable("test-table");
            FileReference file = fileFactory.rootFile("test.parquet", 123L);
            StateStoreCommitRequest commitRequest = StateStoreCommitRequest.forIngestAddFiles(
                    IngestAddFilesCommitRequest.builder()
                            .tableId("test-table")
                            .fileReferences(List.of(file))
                            .build());

            // When
            committer().applyBatch(PollWithRetries.noRetries(), List.of(message(commitRequest)));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(file);
            assertThat(failedRequests).isEmpty();
        }

        @Test
        void shouldTrackFailedRequest() throws Exception {
            // Given
            StateStore stateStore = createTable("test-table");
            FileReference file = fileFactory.rootFile("test.parquet", 123L);
            stateStore.addFile(file);
            FileReference duplicate = fileFactory.rootFile("test.parquet", 123L);
            StateStoreCommitRequest commitRequest = StateStoreCommitRequest.forIngestAddFiles(
                    IngestAddFilesCommitRequest.builder()
                            .tableId("test-table")
                            .fileReferences(List.of(duplicate))
                            .build());

            // When
            committer().applyBatch(PollWithRetries.noRetries(), List.of(message(commitRequest)));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(file);
            assertThat(failedRequests).containsExactly(commitRequest);
        }
    }

    private StateStoreCommitter committer() {
        return committerWithTimes(Instant::now);
    }

    private StateStoreCommitter committerWithTimes(Collection<Instant> times) {
        return committerWithTimes(times.iterator()::next);
    }

    private StateStoreCommitter committerWithTimes(Supplier<Instant> timeSupplier) {
        return new StateStoreCommitter(compactionJobStatusStore, ingestJobStatusStore,
                new FixedTablePropertiesProvider(tables),
                FixedStateStoreProvider.byTableId(stateStoreByTableId),
                timeSupplier);
    }

    private StateStore createTable(String tableId) {
        StateStore stateStore = inMemoryStateStoreWithPartitions(partitions.getAllPartitions());
        stateStore.fixFileUpdateTime(DEFAULT_FILE_UPDATE_TIME);
        createTable(tableId, stateStore);
        return stateStore;
    }

    private void createTable(String tableId, StateStore stateStore) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tables.add(tableProperties);
        stateStoreByTableId.put(tableId, stateStore);
    }

    private StateStore stateStore(String tableId) {
        return stateStoreByTableId.get(tableId);
    }

    private CompactionJobCommitRequest createFinishedCompactionForTable(
            String tableId, Instant createTime, Instant startTime, RecordsProcessedSummary summary) throws Exception {
        CompactionJob job = CompactionJob.builder()
                .tableId(tableId)
                .jobId(UUID.randomUUID().toString())
                .inputFiles(List.of("input.parquet"))
                .outputFile("output.parquet")
                .partitionId("root")
                .build();
        StateStore stateStore = stateStore(tableId);
        stateStore.addFile(fileFactory.rootFile("input.parquet", 123L));
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(
                job.getId(), "root", List.of("input.parquet"))));
        compactionJobStatusStore.jobCreated(job, createTime);
        compactionJobStatusStore.jobStarted(compactionJobStarted(job, startTime)
                .taskId("test-task").jobRunId("test-job-run").build());
        compactionJobStatusStore.jobFinished(compactionJobFinished(job, summary)
                .taskId("test-task").jobRunId("test-job-run").build());
        return new CompactionJobCommitRequest(job, "test-task", "test-job-run", summary);
    }

    private RequestHandle message(StateStoreCommitRequest request) {
        return new RequestHandle(request, () -> failedRequests.add(request));
    }
}
