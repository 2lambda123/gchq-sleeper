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
package sleeper.ingest.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.testutils.RecordGenerator.valueFields;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;

class IngestJobRunnerIT {
    @RegisterExtension
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3);
    private static final String TEST_INSTANCE_NAME = "myinstance";
    private static final String TEST_TABLE_NAME = "mytable";
    private static final String DATA_BUCKET_NAME = "data_bucket";

    private static final String INGEST_DATA_BUCKET_NAME = TEST_INSTANCE_NAME + "-" + TEST_TABLE_NAME + "-ingestdata";
    private static final String TABLE_DATA_BUCKET_NAME = TEST_INSTANCE_NAME + "-" + TEST_TABLE_NAME + "-tabledata";
    @TempDir
    public java.nio.file.Path temporaryFolder;
    private String currentLocalIngestDirectory;
    private String currentLocalTableDataDirectory;


    private static Stream<Arguments> parametersForTests() {
        return Stream.of(
                Arguments.of("arrow", "async", "s3a://"),
                Arguments.of("arrow", "direct", "s3a://"),
                Arguments.of("arrow", "direct", "file://"),
                Arguments.of("arraylist", "async", "s3a://"),
                Arguments.of("arraylist", "direct", "s3a://"),
                Arguments.of("arraylist", "direct", "file://")
        );
    }

    @BeforeEach
    public void before() throws IOException {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(TABLE_DATA_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(INGEST_DATA_BUCKET_NAME);
        currentLocalIngestDirectory = createTempDirectory(temporaryFolder, null).toString();
        currentLocalTableDataDirectory = createTempDirectory(temporaryFolder, null).toString();
    }

    @AfterEach
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    private InstanceProperties getInstanceProperties(String fileSystemPrefix,
                                                     String recordBatchType,
                                                     String partitionFileWriterType) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, TEST_INSTANCE_NAME);
        instanceProperties.set(FILE_SYSTEM, fileSystemPrefix);
        instanceProperties.set(INGEST_RECORD_BATCH_TYPE, recordBatchType);
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, partitionFileWriterType);
        return instanceProperties;
    }

    private TableProperties createTable(Schema schema, String fileSystemPrefix,
                                        String recordBatchType,
                                        String partitionFileWriterType) {
        InstanceProperties instanceProperties = getInstanceProperties(fileSystemPrefix, recordBatchType, partitionFileWriterType);

        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, TEST_TABLE_NAME);
        tableProperties.setSchema(schema);
        tableProperties.set(DATA_BUCKET, getTableDataBucket(fileSystemPrefix));
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, TEST_TABLE_NAME + "-af");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, TEST_TABLE_NAME + "-rfgcf");
        tableProperties.set(PARTITION_TABLENAME, TEST_TABLE_NAME + "-p");

        return tableProperties;
    }

    private String getTableDataBucket(String fileSystemPrefix) {
        switch (fileSystemPrefix.toLowerCase(Locale.ROOT)) {
            case "s3a://":
                return TABLE_DATA_BUCKET_NAME;
            case "file://":
                return currentLocalTableDataDirectory;
            default:
                throw new AssertionError(String.format("File system %s is not supported", fileSystemPrefix));
        }
    }

    private String getIngestBucket(String fileSystemPrefix) {
        switch (fileSystemPrefix.toLowerCase(Locale.ROOT)) {
            case "s3a://":
                return INGEST_DATA_BUCKET_NAME;
            case "file://":
                return currentLocalIngestDirectory;
            default:
                throw new AssertionError(String.format("File system %s is not supported", fileSystemPrefix));
        }
    }

    private List<String> writeParquetFilesForIngest(String fileSystemPrefix,
                                                    RecordGenerator.RecordListAndSchema recordListAndSchema,
                                                    String subDirectory,
                                                    int numberOfFiles) throws IOException {
        List<String> files = new ArrayList<>();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileWithoutSystemPrefix = String.format("%s/%s/file-%d.parquet",
                    getIngestBucket(fileSystemPrefix), subDirectory, fileNo);
            files.add(fileWithoutSystemPrefix);
            Path path = new Path(fileSystemPrefix + fileWithoutSystemPrefix);
            writeParquetFileForIngest(path, recordListAndSchema);
        }

        return files;
    }

    private void writeParquetFileForIngest(
            Path path, RecordGenerator.RecordListAndSchema recordListAndSchema) throws IOException {
        ParquetWriter<Record> writer = ParquetRecordWriterFactory
                .createParquetRecordWriter(path, recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
        for (Record record : recordListAndSchema.recordList) {
            writer.write(record);
        }
        writer.close();
    }

    private IngestCoordinatorTestParameters.Builder createTestParameterBuilder() {
        return IngestCoordinatorTestParameters
                .builder()
                .temporaryFolder(temporaryFolder)
                .awsResource(AWS_EXTERNAL_RESOURCE)
                .dataBucketName(DATA_BUCKET_NAME);
    }

    @ParameterizedTest(name = "backedBy: {0}, writeMode: {1}, fileSystem: {2}")
    @MethodSource("parametersForTests")
    void shouldIngestParquetFiles(String recordBatchType,
                                  String partitionFileWriterType,
                                  String fileSystemPrefix) throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        InstanceProperties instanceProperties = getInstanceProperties(fileSystemPrefix, recordBatchType, partitionFileWriterType);
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(createTable(recordListAndSchema.sleeperSchema, fileSystemPrefix, recordBatchType, partitionFileWriterType));
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);
        StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tablePropertiesProvider.getTableProperties(TEST_TABLE_NAME), stateStore);

        List<String> files = writeParquetFilesForIngest(
                fileSystemPrefix, recordListAndSchema, "", 2);
        List<Record> doubledRecords = Stream.of(recordListAndSchema.recordList, recordListAndSchema.recordList)
                .flatMap(List::stream)
                .sorted(new RecordComparator(recordListAndSchema.sleeperSchema))
                .collect(Collectors.toList());
        // Run the job consumer
        new IngestJobRunner(
                new ObjectFactory(instanceProperties, null, createTempDirectory(temporaryFolder, null).toString()),
                instanceProperties,
                tablePropertiesProvider,
                PropertiesReloader.neverReload(),
                stateStoreProvider,
                localDir,
                AWS_EXTERNAL_RESOURCE.getS3AsyncClient(),
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration())
                .ingest(IngestJob.builder()
                        .tableName(TEST_TABLE_NAME).id("id").files(files)
                        .build());

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();

        stateStore.initialise(tree.getAllPartitions());
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(actualFiles.get(0).getLastStateStoreUpdateTime()))
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(actualFiles.get(0).getFilename(), 20, -5L, 4L)
        );
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(doubledRecords);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest(name = "backedBy: {0}, writeMode: {1}, fileSystem: {2}")
    @MethodSource("parametersForTests")
    void shouldBeAbleToHandleAllFileFormats(String recordBatchType,
                                            String partitionFileWriterType,
                                            String fileSystemPrefix) throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<String> files = writeParquetFilesForIngest(
                fileSystemPrefix, recordListAndSchema, "", 1);
        String ingestBucket = getIngestBucket(fileSystemPrefix);
        URI uri1 = new URI(fileSystemPrefix + ingestBucket + "/file-1.crc");
        FileSystem.get(uri1, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration()).createNewFile(new Path(uri1));
        files.add(ingestBucket + "/file-1.crc");
        URI uri2 = new URI(fileSystemPrefix + ingestBucket + "/file-2.csv");
        FileSystem.get(uri2, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration()).createNewFile(new Path(uri2));
        files.add(ingestBucket + "/file-2.csv");
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        InstanceProperties instanceProperties = getInstanceProperties(fileSystemPrefix, recordBatchType, partitionFileWriterType);
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(createTable(recordListAndSchema.sleeperSchema, fileSystemPrefix, recordBatchType, partitionFileWriterType));
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);
        StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tablePropertiesProvider.getTableProperties(TEST_TABLE_NAME), stateStore);

        new IngestJobRunner(
                new ObjectFactory(instanceProperties, null, createTempDirectory(temporaryFolder, null).toString()),
                instanceProperties,
                tablePropertiesProvider,
                PropertiesReloader.neverReload(),
                stateStoreProvider,
                localDir,
                AWS_EXTERNAL_RESOURCE.getS3AsyncClient(),
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration()).ingest(
                IngestJob.builder()
                        .tableName(TEST_TABLE_NAME).id("id").files(files)
                        .build());
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();

        stateStore.initialise(tree.getAllPartitions());
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(actualFiles.get(0).getLastStateStoreUpdateTime()))
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(actualFiles.get(0).getFilename(), 200, -100L, 99L)
        );
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest(name = "backedBy: {0}, writeMode: {1}, fileSystem:{2}")
    @MethodSource("parametersForTests")
    void shouldIngestParquetFilesInNestedDirectories(String recordBatchType,
                                                     String partitionFileWriterType,
                                                     String fileSystemPrefix) throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        int noOfTopLevelDirectories = 2;
        int noOfNestings = 4;
        int noOfFilesPerDirectory = 2;
        List<String> files = IntStream.range(0, noOfTopLevelDirectories)
                .mapToObj(topLevelDirNo ->
                        IntStream.range(0, noOfNestings).mapToObj(nestingNo -> {
                            try {
                                String dirName = String.format("dir-%d%s", topLevelDirNo, String.join("", Collections.nCopies(nestingNo, "/nested-dir")));
                                return writeParquetFilesForIngest(
                                        fileSystemPrefix, recordListAndSchema, dirName, noOfFilesPerDirectory);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }).flatMap(List::stream).collect(Collectors.toList()))
                .flatMap(List::stream).collect(Collectors.toList());
        List<Record> expectedRecords = Stream.of(Collections.nCopies(noOfTopLevelDirectories * noOfNestings * noOfFilesPerDirectory, recordListAndSchema.recordList))
                .flatMap(List::stream)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        InstanceProperties instanceProperties = getInstanceProperties(fileSystemPrefix, recordBatchType, partitionFileWriterType);
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(createTable(recordListAndSchema.sleeperSchema, fileSystemPrefix, recordBatchType, partitionFileWriterType));
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);
        StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tablePropertiesProvider.getTableProperties(TEST_TABLE_NAME), stateStore);

        new IngestJobRunner(
                new ObjectFactory(instanceProperties, null, createTempDirectory(temporaryFolder, null).toString()),
                instanceProperties,
                tablePropertiesProvider,
                PropertiesReloader.neverReload(),
                stateStoreProvider,
                localDir,
                AWS_EXTERNAL_RESOURCE.getS3AsyncClient(),
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration()).ingest(
                IngestJob.builder()
                        .tableName(TEST_TABLE_NAME).id("id").files(files)
                        .build());
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();

        stateStore.initialise(tree.getAllPartitions());
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(actualFiles.get(0).getLastStateStoreUpdateTime()))
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(actualFiles.get(0).getFilename(), 160, -5L, 4L)
        );
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @Test
    void shouldWriteRecordsFromTwoBuckets() throws Exception {
        RecordGenerator.RecordListAndSchema records1 = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        RecordGenerator.RecordListAndSchema records2 = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(10, 20).boxed().collect(Collectors.toList()));
        writeParquetFileForIngest(new Path("s3a://" + TABLE_DATA_BUCKET_NAME + "/ingest/file1.parquet"), records1);
        writeParquetFileForIngest(new Path("s3a://" + INGEST_DATA_BUCKET_NAME + "/ingest/file2.parquet"), records2);

        IngestJob ingestJob = IngestJob.builder()
                .tableName(TEST_TABLE_NAME).id("id").files(
                        TABLE_DATA_BUCKET_NAME + "/ingest/file1.parquet",
                        INGEST_DATA_BUCKET_NAME + "/ingest/file2.parquet")
                .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(records1.recordList);
        expectedRecords.addAll(records2.recordList);
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        InstanceProperties instanceProperties = getInstanceProperties("s3a://", "arrow", "async");
        TableProperties tableProperties = createTable(records1.sleeperSchema, "s3a://", "arrow", "async");
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(records1.sleeperSchema);
        StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tablePropertiesProvider.getTableProperties(TEST_TABLE_NAME), stateStore);

        // Run the job consumer
        IngestJobRunner ingestJobRunner = new IngestJobRunner(
                new ObjectFactory(instanceProperties, null, createTempDirectory(temporaryFolder, null).toString()),
                instanceProperties,
                tablePropertiesProvider,
                PropertiesReloader.neverReload(),
                stateStoreProvider,
                localDir,
                AWS_EXTERNAL_RESOURCE.getS3AsyncClient(),
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
        ingestJobRunner.ingest(ingestJob);
        PartitionTree tree = new PartitionsBuilder(records1.sleeperSchema)
                .rootFirst("root")
                .buildTree();

        stateStore.initialise(tree.getAllPartitions());
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(records1.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(actualFiles.get(0).getLastStateStoreUpdateTime()))
                .schema(records1.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(actualFiles.get(0).getFilename(), 20, -5L, 19L)
        );
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        List<Record> expectedRecordList = Stream.of(records1.recordList, records2.recordList).flatMap(List::stream).collect(Collectors.toList());
        ResultVerifier.assertOnSketch(
                new Field("key0", new LongType()),
                new RecordGenerator.RecordListAndSchema(expectedRecordList, Schema.builder()
                        .rowKeyFields(new Field("key0", new LongType()))
                        .valueFields(valueFields())
                        .build()),
                actualFiles,
                hadoopConfiguration
        );
    }
}
