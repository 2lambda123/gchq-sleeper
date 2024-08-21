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
package sleeper.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.GetStateStoreByTableId;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequest;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.status.IngestJobAddedFilesEvent;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.compaction.job.status.CompactionJobCommittedEvent.compactionJobCommitted;
import static sleeper.compaction.job.status.CompactionJobFailedEvent.compactionJobFailed;

/**
 * Applies a state store commit request.
 */
public class StateStoreCommitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitter.class);

    private final StateStoreCommitRequestDeserialiser deserialiser;
    private final CompactionJobStatusStore compactionJobStatusStore;
    private final IngestJobStatusStore ingestJobStatusStore;
    private final GetStateStoreByTableId stateStoreProvider;
    private final Supplier<Instant> timeSupplier;

    public StateStoreCommitter(
            TablePropertiesProvider tablePropertiesProvider,
            CompactionJobStatusStore compactionJobStatusStore,
            IngestJobStatusStore ingestJobStatusStore,
            GetStateStoreByTableId stateStoreProvider,
            LoadS3ObjectFromDataBucket loadFromDataBucket,
            Supplier<Instant> timeSupplier) {
        this.deserialiser = new StateStoreCommitRequestDeserialiser(tablePropertiesProvider, loadFromDataBucket);
        this.compactionJobStatusStore = compactionJobStatusStore;
        this.ingestJobStatusStore = ingestJobStatusStore;
        this.stateStoreProvider = stateStoreProvider;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Applies a state store commit request.
     *
     * @param  json the commit request JSON string
     * @return      the commit request
     */
    public StateStoreCommitRequest applyFromJson(String json) throws StateStoreException {
        StateStoreCommitRequest request = deserialiser.fromJson(json);
        apply(request);
        return request;
    }

    /**
     * Applies a state store commit request.
     *
     * @param request the commit request
     */
    public void apply(StateStoreCommitRequest request) throws StateStoreException {
        request.apply(this);
        LOGGER.info("Applied request to table ID {} with type {} at time {}",
                request.getTableId(), request.getRequest().getClass().getSimpleName(), Instant.now());
    }

    void commitCompaction(CompactionJobCommitRequest request) throws StateStoreException {
        CompactionJob job = request.getJob();
        StateStore stateStore = stateStoreProvider.getByTableId(job.getTableId());
        try {
            CompactionJobCommitter.updateStateStoreSuccess(job, request.getRecordsWritten(), stateStore);
        } catch (Exception e) {
            compactionJobStatusStore.jobFailed(compactionJobFailed(job,
                    new ProcessRunTime(request.getFinishTime(), timeSupplier.get()))
                    .failure(e)
                    .taskId(request.getTaskId())
                    .jobRunId(request.getJobRunId())
                    .build());
            throw e;
        }
        compactionJobStatusStore.jobCommitted(compactionJobCommitted(job, timeSupplier.get())
                .taskId(request.getTaskId()).jobRunId(request.getJobRunId()).build());
        LOGGER.debug("Successfully committed compaction job {}", job.getId());
    }

    void addFiles(IngestAddFilesCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStoreProvider.getByTableId(request.getTableId());
        List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(request.getFileReferences());
        stateStore.addFilesWithReferences(files);
        IngestJob job = request.getJob();
        if (job != null) {
            ingestJobStatusStore.jobAddedFiles(IngestJobAddedFilesEvent.ingestJobAddedFiles(job, files, request.getWrittenTime())
                    .taskId(request.getTaskId()).jobRunId(request.getJobRunId()).build());
            LOGGER.debug("Successfully committed new files for ingest job {}", job.getId());
        } else {
            LOGGER.debug("Successfully committed new files for ingest with no job");
        }
    }

    void splitPartition(SplitPartitionCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStoreProvider.getByTableId(request.getTableId());
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(request.getParentPartition(), request.getLeftChild(), request.getRightChild());
    }

    void assignCompactionInputFiles(CompactionJobIdAssignmentCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStoreProvider.getByTableId(request.getTableId());
        stateStore.assignJobIds(request.getAssignJobIdRequests());
    }

    void filesDeleted(GarbageCollectionCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStoreProvider.getByTableId(request.getTableId());
        stateStore.deleteGarbageCollectedFileReferenceCounts(request.getFilenames());
    }

    /**
     * Loads S3 objects from the data bucket.
     */
    public interface LoadS3ObjectFromDataBucket {
        /**
         * Loads the content of an S3 object.
         *
         * @param  key the key in the data bucket
         * @return     the content
         */
        String loadFromDataBucket(String key);
    }
}
