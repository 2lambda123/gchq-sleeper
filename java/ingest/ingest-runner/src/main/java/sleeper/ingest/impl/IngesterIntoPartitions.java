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
package sleeper.ingest.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.range.Range;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter;
import sleeper.statestore.FileInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * This class writes {@link Record} objects, which must have been sorted before they are passed to this class, into
 * Sleeper partition files. The actual write is performed by {@link PartitionFileWriter} classes and a factory function
 * to generate these is provided when this class is constructed.
 */
class IngesterIntoPartitions {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngesterIntoPartitions.class);

    private final Function<Partition, PartitionFileWriter> partitionFileWriterFactoryFn;
    private final Schema sleeperSchema;

    /**
     * Construct this {@link IngesterIntoPartitions} class.
     *
     * @param sleeperSchema                The Sleeper schema
     * @param partitionFileWriterFactoryFn A function which takes a {@link Partition} and returns the {@link
     *                                     PartitionFileWriter} which will write {@link Record} objects to that
     *                                     partition.
     */
    IngesterIntoPartitions(
            Schema sleeperSchema,
            Function<Partition, PartitionFileWriter> partitionFileWriterFactoryFn) {
        this.partitionFileWriterFactoryFn = requireNonNull(partitionFileWriterFactoryFn);
        this.sleeperSchema = requireNonNull(sleeperSchema);
    }

    /**
     * Close several {@link PartitionFileWriter} objects at once, rethrowing any errors as unchecked exceptions.
     *
     * @param partitionFileWriters The {@link PartitionFileWriter} objects to close
     * @return The {@link CompletableFuture} objects corresponding to the closed {@link PartitionFileWriter} objects, in
     * the same order
     * @throws IOException Thrown when an IO error has occurred. May contain multiple suppressed exceptions, one for
     *                     each {@link PartitionFileWriter} that fails as it is closed.
     */
    private static List<CompletableFuture<FileInfo>> closeMultiplePartitionFileWriters(
            Collection<PartitionFileWriter> partitionFileWriters) throws IOException {
        List<Exception> exceptionList = new ArrayList<>();
        List<CompletableFuture<FileInfo>> futures = partitionFileWriters.stream()
                .map(partitionFileWriter -> {
                    try {
                        return partitionFileWriter.close();
                    } catch (Exception e) {
                        exceptionList.add(e);
                        return CompletableFuture.<FileInfo>completedFuture(null);
                    }
                }).collect(Collectors.toList());
        if (exceptionList.isEmpty()) {
            return futures;
        } else {
            IOException aggregateIOException = new IOException();
            exceptionList.forEach(aggregateIOException::addSuppressed);
            throw aggregateIOException;
        }
    }

    /**
     * Initiate the ingest of the {@link Record} objects passed as a {@link CloseableIterator}. The records must be
     * supplied in sort-order. When this method returns, all of the records will have been read from the iterator and
     * the iterator may be discarded by the caller.
     *
     * @param orderedRecordIterator The {@link Record} objects to write, passed in sort order
     * @param partitionTree         The {@link PartitionTree} to used to determine which partition to place each record
     *                              in
     * @return A {@link CompletableFuture} which completes to return a list of {@link FileInfo} objects, one for each
     * partition file that has been created
     * @throws IOException -
     */
    public CompletableFuture<List<FileInfo>> initiateIngest(CloseableIterator<Record> orderedRecordIterator,
                                                            PartitionTree partitionTree) throws IOException {
        List<String> rowKeyNames = sleeperSchema.getRowKeyFieldNames();
        String firstDimensionRowKey = rowKeyNames.get(0);
        Map<String, PartitionFileWriter> partitionIdToFileWriterMap = new HashMap<>();
        Range currentFirstDimensionRange = null;
        // Set up various flags, counters and the like which will be updated as the write progresses
        Partition currentPartition = null;
        PartitionFileWriter currentPartitionFileWriter = null;
        // Prepare arrays to hold the results
        List<CompletableFuture<FileInfo>> completableFutures = new ArrayList<>();

        // Log and return if the iterator is empty
        if (!orderedRecordIterator.hasNext()) {
            LOGGER.info("There are no records");
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        // Loop through the iterator, creating new partition files whenever this is required.
        // The records are in sort-order and this means that all of the partitions which share the same
        // first dimension range are created at once, and then they may be closed as soon as the records no
        // longer sit inside that first dimension range.
        try {
            while (orderedRecordIterator.hasNext()) {
                Record record = orderedRecordIterator.next();
                Key key = Key.create(record.getValues(rowKeyNames));
                // Ensure that the current partition is the correct one for the new record
                if (currentPartition == null || !currentPartition.isRowKeyInPartition(sleeperSchema, key)) {
                    // Close all of the current partition file writers if the first dimension has changed.
                    if (currentFirstDimensionRange != null &&
                            !currentFirstDimensionRange.doesRangeContainObject(record.get(firstDimensionRowKey))) {
                        completableFutures.addAll(closeMultiplePartitionFileWriters(partitionIdToFileWriterMap.values()));
                        partitionIdToFileWriterMap.clear();
                    }
                    currentPartition = partitionTree.getLeafPartition(key);
                    currentFirstDimensionRange = currentPartition.getRegion().getRange(firstDimensionRowKey);
                    // Create a new partition file writer if required
                    if (!partitionIdToFileWriterMap.containsKey(currentPartition.getId())) {
                        partitionIdToFileWriterMap.put(currentPartition.getId(), partitionFileWriterFactoryFn.apply(currentPartition));
                    }
                    currentPartitionFileWriter = partitionIdToFileWriterMap.get(currentPartition.getId());
                }
                // Write records to the current partition file writer
                currentPartitionFileWriter.append(record);
            }
            completableFutures.addAll(closeMultiplePartitionFileWriters(partitionIdToFileWriterMap.values()));
        } catch (Exception e) {
            partitionIdToFileWriterMap.values().forEach(PartitionFileWriter::abort);
            throw e;
        }

        // Create a future where all of the partitions have finished uploading and then return the FileInfo
        // objects as a list
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                .thenApply(dummy -> completableFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }
}
