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
package sleeper.ingest.impl.partitionfilewriter;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.statestore.FileInfo;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * This class writes partition files to S3 in an asynchronous manner.
 * <ul>
 *     <li>Data is provided to this class, in sort order, through the {@link #append} method. </li>
 *     <li>As the records arrive, local Parquet files are created for each partition. As the records are in sorted order, there will be first be records for one partition, then records for another partition, and so on. (See note below)</li>
 *     <li>As each Parquet partition file is completed, an asynchronous upload to S3 is initiated, which will delete the local copy of the Parquet partition file once the upload has completed</li>
 *     <li>This whole process repeats until {@link #close()} is called, at which point the remaining partition file is uploaded</li>
 *     <li>The {@link #close()} method returns a future which will complete once all of the Parquet partition files have been uploaded</li>
 * </ul>
 * <p>
 * Note that the sort-order and the partition-order may not be the same when the Sleeper Schema has more than one row key.
 * This is the responsibility of the calling classes to handle and this class can assume that all of the records
 * that it receives belong to the same partition.
 */
public class AsyncS3PartitionFileWriter implements PartitionFileWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncS3PartitionFileWriter.class);

    private final S3AsyncClient s3AsyncClient;
    private final Schema sleeperSchema;
    private final Partition partition;
    private final String s3BucketName;
    private final Configuration hadoopConfiguration;
    private final String partitionParquetLocalFileName;
    private final String partitionParquetS3Key;
    private final String quantileSketchesLocalFileName;
    private final String quantileSketchesS3Key;
    private final ParquetWriter<Record> parquetWriter;
    private final Map<String, ItemsSketch> keyFieldToSketchMap;
    private final String rowKeyName;
    private long linesWrittenToCurrentPartition;
    private Object currentPartitionMinKey;
    private Object currentPartitionMaxKey;

    /**
     * Warning: this constructor allows a bespoke Hadoop configuration to be specified, but it will not always be used
     * due an underlying cache in the underlying {@link org.apache.hadoop.fs.FileSystem} object. This {@link org.apache.hadoop.fs.FileSystem} object maintains a
     * cache of file systems and the first time that it creates a {@link org.apache.hadoop.fs.s3a.S3AFileSystem} object,
     * the provided Hadoop configuration will be used. Thereafter, the Hadoop configuration will be ignored until {@link
     * org.apache.hadoop.fs.FileSystem#closeAll()} is called. This is strange behaviour and can cause errors which are difficult to
     * diagnose.
     *
     * @param sleeperSchema           The schema of the records in the partition
     * @param partition               The partition to write to
     * @param s3AsyncClient           The S3 client to use to perform the asynchronous upload
     * @param localWorkingDirectory   The local directory to use to create temporary files
     * @param s3BucketName            The S3 bucket to write to
     * @param parquetCompressionCodec The compression codec to use
     * @param parquetRowGroupSize     The row group size
     * @param parquetPageSize         The page size
     * @param hadoopConfiguration     The Hadoop configuration to use when writing the local Parquet partition file
     * @throws IOException -
     */
    public AsyncS3PartitionFileWriter(Schema sleeperSchema,
                                      Partition partition,
                                      int parquetRowGroupSize,
                                      int parquetPageSize,
                                      String parquetCompressionCodec,
                                      Configuration hadoopConfiguration,
                                      String s3BucketName,
                                      S3AsyncClient s3AsyncClient,
                                      String localWorkingDirectory) throws IOException {
        this.s3AsyncClient = requireNonNull(s3AsyncClient);
        this.sleeperSchema = requireNonNull(sleeperSchema);
        this.partition = requireNonNull(partition);
        this.s3BucketName = requireNonNull(s3BucketName);
        this.hadoopConfiguration = requireNonNull(hadoopConfiguration);
        UUID uuid = UUID.randomUUID();
        this.partitionParquetLocalFileName = String.format("%s/partition_%s_%s.parquet", localWorkingDirectory, partition.getId(), uuid);
        this.quantileSketchesLocalFileName = String.format("%s/partition_%s_%s.sketches", localWorkingDirectory, partition.getId(), uuid);
        this.partitionParquetS3Key = String.format("partition_%s/%s.parquet", partition.getId(), uuid);
        this.quantileSketchesS3Key = String.format("partition_%s/%s.sketches", partition.getId(), uuid);
        this.parquetWriter = createParquetWriter(
                partitionParquetLocalFileName,
                sleeperSchema,
                parquetCompressionCodec,
                parquetRowGroupSize,
                parquetPageSize,
                hadoopConfiguration);
        LOGGER.info("Created Parquet writer for partition {}", partition.getId());
        this.keyFieldToSketchMap = createKeyFieldToSketchMap(sleeperSchema);
        this.rowKeyName = this.sleeperSchema.getRowKeyFields().get(0).getName();
        this.linesWrittenToCurrentPartition = 0L;
        this.currentPartitionMinKey = null;
        this.currentPartitionMaxKey = null;
    }

    /**
     * Create a {@link FileInfo} object from the values supplied
     *
     * @param sleeperSchema -
     * @param filename      -
     * @param partitionId   -
     * @param numberOfLines -
     * @param minKey        -
     * @param maxKey        -
     * @param updateTime    -
     * @return The {@link FileInfo} object
     */
    private static FileInfo createFileInfo(
            Schema sleeperSchema,
            String filename,
            String partitionId,
            long numberOfLines,
            Object minKey,
            Object maxKey,
            long updateTime) {
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(sleeperSchema.getRowKeyTypes());
        fileInfo.setFilename(filename);
        fileInfo.setPartitionId(partitionId);
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setNumberOfRecords(numberOfLines);
        fileInfo.setMinRowKey(Key.create(minKey));
        fileInfo.setMaxRowKey(Key.create(maxKey));
        fileInfo.setLastStateStoreUpdateTime(updateTime);
        return fileInfo;
    }

    /**
     * Create a Parquet writer to write Records to the named file
     *
     * @param outputFile              The name of the Parquet file to write to
     * @param sleeperSchema           -
     * @param parquetCompressionCodec -
     * @param parquetRowGroupSize     -
     * @param parquetPageSize         -
     * @param hadoopConfiguration     -
     * @return The {@link ParquetWriter} object
     * @throws IOException -
     */
    private static ParquetWriter<Record> createParquetWriter(String outputFile,
                                                             Schema sleeperSchema,
                                                             String parquetCompressionCodec,
                                                             int parquetRowGroupSize,
                                                             int parquetPageSize,
                                                             Configuration hadoopConfiguration) throws IOException {
        ParquetRecordWriter.Builder builder = new ParquetRecordWriter.Builder(new Path(outputFile),
                SchemaConverter.getSchema(sleeperSchema), sleeperSchema)
                .withCompressionCodec(CompressionCodecName.fromConf(parquetCompressionCodec))
                .withRowGroupSize(parquetRowGroupSize)
                .withPageSize(parquetPageSize)
                .withConf(hadoopConfiguration);
        return builder.build();
    }

    /**
     * Create a {@link CompletableFuture} which uploads the named file, asynchronously, to S3 and then deletes the local
     * copy of that file. The future completes once the file has been deleted and it contains the {@link
     * PutObjectResponse} which was returned as the file was uploaded.
     *
     * @param s3AsyncClient       The client to use to perform the asynchronous upload
     * @param localFileName       The file to upload
     * @param s3BucketName        The S3 bucket to put the file into
     * @param s3Key               The S3 key of the uploaded file
     * @param hadoopConfiguration The Hadoop configuration to use when deleting the local file
     * @return The {@link CompletableFuture} which was returned by the {@link
     * S3AsyncClient#putObject} method.
     */
    private static CompletableFuture<PutObjectResponse> asyncUploadLocalFileToS3ThenDeleteLocalCopy(
            S3AsyncClient s3AsyncClient,
            String localFileName,
            String s3BucketName,
            String s3Key,
            Configuration hadoopConfiguration) {
        File localFile = new File(localFileName);
        CompletableFuture<PutObjectResponse> uploadFuture = s3AsyncClient.putObject(
                PutObjectRequest.builder()
                        .bucket(s3BucketName)
                        .key(s3Key)
                        .build(),
                AsyncRequestBody.fromFile(localFile));
        LOGGER.debug("Started asynchronous upload of local file {} to s3://{}/{}", localFileName, s3BucketName, s3Key);
        return uploadFuture.whenComplete((msg, ex) -> {
            LOGGER.debug("Completed asynchronous upload of local file {} to s3://{}/{}", localFileName, s3BucketName, s3Key);
            Path path = new Path(localFileName);
            boolean successfulDelete = false;
            try {
                successfulDelete = path.getFileSystem(hadoopConfiguration).delete(path, false);
            } catch (IOException e) {
                LOGGER.error("Error whilst deleting {}: {}", path, e);
            }
            if (!successfulDelete) {
                LOGGER.warn("Failed to delete local file {}", localFileName);
            }
        });
    }

    /**
     * Create a map from the name of each row-key to an empty sketch.
     *
     * @param sleeperSchema The schema that contains the row keys
     * @return The map
     */
    private static Map<String, ItemsSketch> createKeyFieldToSketchMap(Schema sleeperSchema) {
        return sleeperSchema.getRowKeyFields().stream()
                .collect(Collectors.toMap(
                        Field::getName,
                        rowKeyField -> ItemsSketch.getInstance(1024, Comparator.naturalOrder())));
    }

    /**
     * Update all of the sketches in the supplied map with the values taken from the supplied record.
     *
     * @param keyFieldToSketchMap The map to update
     * @param sleeperSchema       The schema of the corresponding record
     * @param record              The record to take the values from
     */
    private static void updateKeyFieldToSketchMap(
            Map<String, ItemsSketch> keyFieldToSketchMap,
            Schema sleeperSchema,
            Record record) {
        for (Field rowKeyField : sleeperSchema.getRowKeyFields()) {
            if (rowKeyField.getType() instanceof ByteArrayType) {
                byte[] value = (byte[]) record.get(rowKeyField.getName());
                keyFieldToSketchMap.get(rowKeyField.getName()).update(ByteArray.wrap(value));
            } else {
                Object value = record.get(rowKeyField.getName());
                keyFieldToSketchMap.get(rowKeyField.getName()).update(value);
            }
        }
    }

    /**
     * Append a record to the partition. This writes the record to a local Parquet file and does not upload it to S3.
     *
     * @param record The record to append
     * @throws IOException -
     */
    @Override
    public void append(Record record) throws IOException {
        parquetWriter.write(record);
        updateKeyFieldToSketchMap(keyFieldToSketchMap, sleeperSchema, record);
        if (currentPartitionMinKey == null) {
            currentPartitionMinKey = record.get(rowKeyName);
        }
        currentPartitionMaxKey = record.get(rowKeyName);
        linesWrittenToCurrentPartition++;
        if (linesWrittenToCurrentPartition % 1000000 == 0) {
            LOGGER.info("Written {} rows to partition {}", linesWrittenToCurrentPartition, partition.getId());
        }
    }

    /**
     * Close this partition writer. The local Parquet file is closed and then an asynchronous upload to S3 is initiated,
     * for both the Parquet file and for the associated quantiles sketch file. The local copies are deleted and then the
     * {@link CompletableFuture} completes. The details of new partition file are returned in the completed future.
     *
     * @return Details about the new partition file
     * @throws IOException -
     */
    @Override
    public CompletableFuture<FileInfo> close() throws IOException {
        parquetWriter.close();
        LOGGER.debug("Closed writer for local partition {} after writing {} rows: file {}",
                partition.getId(),
                linesWrittenToCurrentPartition,
                partitionParquetLocalFileName);
        // Write sketches to a local file
        new SketchesSerDeToS3(sleeperSchema).saveToHadoopFS(
                new Path(quantileSketchesLocalFileName),
                new Sketches(keyFieldToSketchMap),
                hadoopConfiguration);
        LOGGER.debug("Wrote sketches to local file {}", quantileSketchesLocalFileName);
        FileInfo fileInfo = createFileInfo(
                sleeperSchema,
                String.format("s3a://%s/%s", s3BucketName, partitionParquetS3Key),
                partition.getId(),
                linesWrittenToCurrentPartition,
                currentPartitionMinKey,
                currentPartitionMaxKey,
                System.currentTimeMillis());
        // Start the asynchronous upload of the files to S3
        CompletableFuture<?> partitionFileUploadFuture = asyncUploadLocalFileToS3ThenDeleteLocalCopy(
                s3AsyncClient,
                partitionParquetLocalFileName,
                s3BucketName,
                partitionParquetS3Key,
                hadoopConfiguration);
        CompletableFuture<?> quantileFileUploadFuture = asyncUploadLocalFileToS3ThenDeleteLocalCopy(
                s3AsyncClient,
                quantileSketchesLocalFileName,
                s3BucketName,
                quantileSketchesS3Key,
                hadoopConfiguration);
        return CompletableFuture.allOf(partitionFileUploadFuture, quantileFileUploadFuture)
                .thenApply(dummy -> fileInfo);
    }

    /**
     * Make a best-effort attempt to delete local files and free up resources.
     */
    @Override
    public void abort() {
        try {
            parquetWriter.close();
        } catch (Exception e) {
            LOGGER.error("Error aborting ParquetWriter", e);
        }
        if (!((new File(partitionParquetLocalFileName)).delete())) {
            LOGGER.error("Failed to delete " + partitionParquetLocalFileName);
        }
        if (!((new File(quantileSketchesLocalFileName)).delete())) {
            LOGGER.error("Failed to delete " + quantileSketchesLocalFileName);
        }
    }
}
