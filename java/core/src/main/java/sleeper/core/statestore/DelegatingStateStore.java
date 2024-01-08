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
package sleeper.core.statestore;

import sleeper.core.partition.Partition;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class DelegatingStateStore implements StateStore {
    private final FileInfoStore fileInfoStore;
    private final PartitionStore partitionStore;

    public DelegatingStateStore(FileInfoStore fileInfoStore, PartitionStore partitionStore) {
        this.fileInfoStore = fileInfoStore;
        this.partitionStore = partitionStore;
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        fileInfoStore.addFile(fileInfo);
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        fileInfoStore.addFiles(fileInfos);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(String partitionId, List<String> filesToBeMarkedReadyForGC, List<FileInfo> newFiles) throws StateStoreException {
        fileInfoStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(partitionId, filesToBeMarkedReadyForGC, newFiles);
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        fileInfoStore.atomicallyUpdateJobStatusOfFiles(jobId, fileInfos);
    }

    @Override
    public void deleteReadyForGCFiles(List<String> filenames) throws StateStoreException {
        fileInfoStore.deleteReadyForGCFiles(filenames);
    }

    @Override
    public List<FileInfo> getActiveFiles() throws StateStoreException {
        return fileInfoStore.getActiveFiles();
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        return fileInfoStore.getReadyForGCFilenamesBefore(maxUpdateTime);
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        return fileInfoStore.getActiveFilesWithNoJobId();
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        return fileInfoStore.getPartitionToActiveFilesMap();
    }

    @Override
    public AllFileReferences getAllFileReferencesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        return fileInfoStore.getAllFileReferencesWithMaxUnreferenced(maxUnreferencedFiles);
    }

    @Override
    public void initialise() throws StateStoreException {
        if (!hasNoFiles()) {
            throw new StateStoreException("Cannot initialise state store when files are present");
        }
        partitionStore.initialise();
        fileInfoStore.initialise();
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        if (!hasNoFiles()) {
            throw new StateStoreException("Cannot initialise state store when files are present");
        }
        partitionStore.initialise(partitions);
        fileInfoStore.initialise();
    }

    public void setInitialFileInfos() throws StateStoreException {
        fileInfoStore.initialise();
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
        partitionStore.atomicallyUpdatePartitionAndCreateNewOnes(splitPartition, newPartition1, newPartition2);
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        return partitionStore.getAllPartitions();
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitionStore.getLeafPartitions();
    }

    @Override
    public boolean hasNoFiles() {
        return fileInfoStore.hasNoFiles();
    }

    @Override
    public void clearFileData() {
        fileInfoStore.clearFileData();
    }

    @Override
    public void clearPartitionData() {
        partitionStore.clearPartitionData();
    }

    @Override
    public void fixTime(Instant now) {
        fileInfoStore.fixTime(now);
    }
}
