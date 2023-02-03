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
package sleeper.ingest.impl.recordbatch.arraylist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class ArrayListRecordBatch<INCOMINGDATATYPE> implements RecordBatch<INCOMINGDATATYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrayListRecordBatch.class);
    private final ParquetConfiguration parquetConfiguration;
    private final Schema sleeperSchema;
    private final ArrayListRecordMapper<INCOMINGDATATYPE> recordMapper;
    private final String localWorkingDirectory;
    private final int maxNoOfRecordsInMemory;
    private final long maxNoOfRecordsInLocalStore;
    private final Configuration hadoopConfiguration;
    private final UUID uniqueIdentifier;
    private final List<Record> inMemoryBatch;
    private final List<String> localFileNames;
    private long noOfRecordsInLocalStore;
    private CloseableIterator<Record> internalOrderedRecordIterator;
    private boolean isWriteable;
    private int batchNo;

    /**
     * Construct the ArrayList-based batch of records.
     *
     * @param parquetConfiguration       Hadoop, schema and Parquet configuration for writing files.
     *                                   The Hadoop configuration is used during read and write of the Parquet files.
     *                                   Note that the library code uses caching and so unusual errors can occur if
     *                                   different configurations are used in different calls.
     * @param localWorkingDirectory      A local directory to use to store temporary files
     * @param maxNoOfRecordsInMemory     The maximum number of records to store in the internal ArrayList
     * @param maxNoOfRecordsInLocalStore The maximum number of records to store on the local disk
     */
    public ArrayListRecordBatch(ParquetConfiguration parquetConfiguration,
                                ArrayListRecordMapper<INCOMINGDATATYPE> recordMapper,
                                String localWorkingDirectory,
                                int maxNoOfRecordsInMemory,
                                long maxNoOfRecordsInLocalStore) {
        this.parquetConfiguration = requireNonNull(parquetConfiguration);
        this.sleeperSchema = parquetConfiguration.getSleeperSchema();
        this.recordMapper = recordMapper;
        this.localWorkingDirectory = requireNonNull(localWorkingDirectory);
        this.maxNoOfRecordsInMemory = maxNoOfRecordsInMemory;
        this.maxNoOfRecordsInLocalStore = maxNoOfRecordsInLocalStore;
        this.hadoopConfiguration = parquetConfiguration.getHadoopConfiguration();
        this.uniqueIdentifier = UUID.randomUUID();
        this.internalOrderedRecordIterator = null;
        this.isWriteable = true;
        this.inMemoryBatch = new ArrayList<>(maxNoOfRecordsInMemory);
        this.noOfRecordsInLocalStore = 0L;
        this.batchNo = 0;
        this.localFileNames = new ArrayList<>();
    }

    /**
     * Internal method to add a record to the internal batch, flushing to local disk first if necessary.
     *
     * @param record The record to add to the batch
     * @throws IOException -
     */
    protected void addRecordToBatch(Record record) throws IOException {
        if (!isWriteable) {
            throw new AssertionError("Attempt to write to a batch where an iterator has already been created");
        }
        if (inMemoryBatch.size() >= maxNoOfRecordsInMemory) {
            flushToLocalDiskAndClear();
        }
        inMemoryBatch.add(record);
    }

    /**
     * Flushes the in-memory batch of records to a local file and then clears the in-memory batch.
     *
     * @throws IOException -
     */
    private void flushToLocalDiskAndClear() throws IOException {
        if (inMemoryBatch.isEmpty()) {
            LOGGER.info("There are no records to flush");
        } else {
            long time1 = System.currentTimeMillis();
            String outputFileName = String.format("%s/localfile-batch-%s-file-%09d.parquet",
                    localWorkingDirectory,
                    uniqueIdentifier,
                    batchNo);
            inMemoryBatch.sort(new RecordComparator(sleeperSchema));
            long time2 = System.currentTimeMillis();
            // Write the records to a local Parquet file. The try-with-resources block ensures that the writer
            // is closed in both success and failure.
            try (ParquetWriter<Record> parquetWriter = parquetConfiguration.createParquetWriter(outputFileName)) {
                for (Record record : inMemoryBatch) {
                    parquetWriter.write(record);
                }
            }
            long time3 = System.currentTimeMillis();
            LOGGER.info(String.format("Wrote %d records to local file in %.1fs (%.1f/s) [sorting %.1fs (%.1f/s), writing %.1fs (%.1f/s)] - filename: %s",
                    inMemoryBatch.size(),
                    (time3 - time1) / 1000.0,
                    inMemoryBatch.size() / ((time3 - time1) / 1000.0),
                    (time2 - time1) / 1000.0,
                    inMemoryBatch.size() / ((time2 - time1) / 1000.0),
                    (time3 - time2) / 1000.0,
                    inMemoryBatch.size() / ((time3 - time2) / 1000.0),
                    outputFileName));
            localFileNames.add(outputFileName);
            noOfRecordsInLocalStore += inMemoryBatch.size();
        }
        batchNo++;
        inMemoryBatch.clear();
    }

    @Override
    public void append(INCOMINGDATATYPE data) throws IOException {
        addRecordToBatch(recordMapper.map(data));
    }

    /**
     * This batch is considered full when the in-memory batch is full and when, if it were written to disk, the total
     * number of records on disk would exceed the limit specified during construction.
     *
     * @return A flag indicating whether or not the batch is full.
     */
    @Override
    public boolean isFull() {
        return inMemoryBatch.size() >= maxNoOfRecordsInMemory &&
                (noOfRecordsInLocalStore + inMemoryBatch.size()) >= maxNoOfRecordsInLocalStore;
    }

    /**
     * Use a merge-sort to merge together all of the sorted files on the local disk and the (sorted) in-memory batch.
     * <p>
     * Note that this method may only be called once.
     *
     * @return An iterator of the sorted records.
     * @throws IOException -
     */
    @Override
    public CloseableIterator<Record> createOrderedRecordIterator() throws IOException {
        if (!isWriteable || (internalOrderedRecordIterator != null)) {
            throw new AssertionError("Attempt to create an iterator where an iterator has already been created");
        }
        isWriteable = false;
        // Flush the current in-memory batch to disk, to free up as much memory as possible for the merge
        flushToLocalDiskAndClear();
        // Create an iterator for each one of the local Parquet files
        List<CloseableIterator<Record>> inputIterators = new ArrayList<>();
        try {
            for (String localFileName : localFileNames) {
                ParquetReader<Record> readerForBatch = createParquetReader(localFileName);
                ParquetReaderIterator recordIterator = new ParquetReaderIterator(readerForBatch);
                inputIterators.add(recordIterator);
                LOGGER.info("Created reader for file {}", localFileName);
            }
        } catch (Exception e1) {
            // Clean up the iterators that have been created
            // Rely on the caller to call close() to delete local files
            inputIterators.forEach(inputIterator -> {
                try {
                    inputIterator.close();
                } catch (Exception e2) {
                    LOGGER.error("Error closing iterator", e2);
                }
            });
            throw e1;
        }
        // Merge into one sorted iterator
        internalOrderedRecordIterator = new MergingIterator(sleeperSchema, inputIterators);
        return internalOrderedRecordIterator;
    }

    /**
     * Close this batch, remove all local files and free resources
     */
    @Override
    public void close() {
        deleteAllLocalFiles();
        try {
            internalOrderedRecordIterator.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a {@link ParquetReader} using the parameters specified during construction.
     *
     * @param inputFile The Parquet file to read
     * @return The {@link ParquetReader}
     * @throws IOException Thrown when the reader cannot be created
     */
    private ParquetReader<Record> createParquetReader(String inputFile) throws IOException {
        ParquetReader.Builder<Record> builder = new ParquetRecordReader.Builder(new Path(inputFile), sleeperSchema)
                .withConf(hadoopConfiguration);
        return builder.build();
    }

    /**
     * Delete all of the local files. Errors are logged but are not propagated.
     */
    private void deleteAllLocalFiles() {
        if (localFileNames.size() > 0) {
            LOGGER.info("Deleting {} local batch files, first: {} last: {}",
                    localFileNames.size(),
                    localFileNames.get(0),
                    localFileNames.get(localFileNames.size() - 1));
            localFileNames.forEach(localFileName -> {
                try {
                    boolean success = FileSystem.getLocal(hadoopConfiguration).delete(new Path(localFileName), false);
                    if (!success) {
                        LOGGER.error("Failed to delete local file {}", localFileName);
                    }
                } catch (IOException e) {
                    LOGGER.error("Failed to delete local file " + localFileName, e);
                }
            });
        }
        localFileNames.clear();
    }

}
