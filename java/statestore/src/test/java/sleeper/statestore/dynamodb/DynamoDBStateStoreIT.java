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
package sleeper.statestore.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.AllReferencesToAFileTestHelper;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.SplitRequestsFailedException;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.dynamodb.exception.OneOrMoreFilesNotFoundException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.noFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.partialReadyForGCFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.readyForGCFilesReport;
import static sleeper.core.statestore.SplitFileReferenceRequest.splitFileToChildPartitions;

public class DynamoDBStateStoreIT extends DynamoDBStateStoreTestBase {

    @Nested
    @DisplayName("Active files")
    class ActiveFiles {
        @Test
        public void shouldReturnCorrectFileReferenceForLongRowKey() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore stateStore = getStateStore(schema);
            FileReference fileReference = FileReference.builder()
                    .filename("abc")
                    .numberOfRecords(100L)
                    .partitionId("1")
                    .countApproximate(false)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

            // When
            stateStore.addFile(fileReference);

            // Then
            assertThat(stateStore.getFileReferences()).singleElement().satisfies(found -> {
                assertThat(found.getFilename()).isEqualTo("abc");
                assertThat(found.getPartitionId()).isEqualTo("1");
                assertThat(found.getLastStateStoreUpdateTime()).isEqualTo(Instant.ofEpochMilli(1_000_000L));
            });
        }

        @Test
        public void shouldReturnCorrectFileReferenceForByteArrayKey() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
            StateStore stateStore = getStateStore(schema);
            FileReference fileReference = FileReference.builder()
                    .filename("abc")
                    .partitionId("1")
                    .numberOfRecords(100L)
                    .countApproximate(false)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

            // When
            stateStore.addFile(fileReference);

            // Then
            assertThat(stateStore.getFileReferences()).singleElement().satisfies(found -> {
                assertThat(found.getFilename()).isEqualTo("abc");
                assertThat(found.getPartitionId()).isEqualTo("1");
                assertThat(found.getLastStateStoreUpdateTime()).isEqualTo(Instant.ofEpochMilli(1_000_000L));
            });
        }

        @Test
        public void shouldReturnCorrectFileReferenceFor2DimensionalByteArrayKey() throws StateStoreException {
            // Given
            Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
            StateStore stateStore = getStateStore(schema);
            FileReference fileReference = FileReference.builder()
                    .filename("abc")
                    .partitionId("1")
                    .numberOfRecords(100L)
                    .countApproximate(false)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

            // When
            stateStore.addFile(fileReference);

            // Then
            assertThat(stateStore.getFileReferences()).singleElement().satisfies(found -> {
                assertThat(found.getFilename()).isEqualTo("abc");
                assertThat(found.getPartitionId()).isEqualTo("1");
                assertThat(found.getLastStateStoreUpdateTime()).isEqualTo(Instant.ofEpochMilli(1_000_000L));
            });
        }

        @Test
        public void shouldReturnCorrectFileReferenceForMultidimensionalRowKey() throws StateStoreException {
            // Given
            Schema schema = schemaWithTwoRowKeyTypes(new LongType(), new StringType());
            StateStore stateStore = getStateStore(schema);
            FileReference fileReference = FileReference.builder()
                    .filename("abc")
                    .partitionId("1")
                    .numberOfRecords(100L)
                    .countApproximate(false)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

            // When
            stateStore.addFile(fileReference);

            // Then
            assertThat(stateStore.getFileReferences()).singleElement().satisfies(found -> {
                assertThat(found.getFilename()).isEqualTo("abc");
                assertThat(found.getPartitionId()).isEqualTo("1");
                assertThat(found.getLastStateStoreUpdateTime()).isEqualTo(Instant.ofEpochMilli(1_000_000L));
            });
        }

        @Test
        public void shouldReturnAllFileReferences() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore stateStore = getStateStore(schema);
            stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
            Set<FileReference> expected = new HashSet<>();
            for (int i = 0; i < 11; i++) {
                FileReference fileReference = fileReferenceFactory.partitionFile("root", "file-" + i, 100L);
                stateStore.addFile(fileReference);
                expected.add(fileReference.toBuilder().lastStateStoreUpdateTime(Instant.ofEpochMilli(1_000_000L)).build());
            }

            // When
            List<FileReference> fileReferences = stateStore.getFileReferences();

            // Then
            assertThat(new HashSet<>(fileReferences)).isEqualTo(expected);
        }

        @Test
        void shouldStoreAndReturnPartialFile() throws Exception {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore stateStore = getStateStore(schema);
            stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));
            FileReference fileReference = FileReference.builder()
                    .filename("partial-file")
                    .partitionId("A")
                    .numberOfRecords(100L)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            stateStore.addFile(fileReference);

            // When
            List<FileReference> fileReferences = stateStore.getFileReferences();

            // Then
            assertThat(fileReferences).containsExactly(fileReference.toBuilder()
                    .lastStateStoreUpdateTime(Instant.ofEpochMilli(1_000_000L))
                    .build());
        }

        @Test
        public void shouldReturnOnlyActiveFilesWithNoJobId() throws StateStoreException {
            // Given
            Schema schema = schemaWithKeyAndValueWithTypes(new LongType(), new StringType());
            DynamoDBStateStore stateStore = getStateStore(schema);
            FileReference fileReference1 = FileReference.builder()
                    .filename("file1")
                    .partitionId("1")
                    .numberOfRecords(100L)
                    .countApproximate(false)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            stateStore.addFile(fileReference1);
            FileReference fileReference2 = FileReference.builder()
                    .filename("file2")
                    .partitionId("2")
                    .numberOfRecords(100L)
                    .countApproximate(false)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            stateStore.addFile(fileReference2);
            FileReference fileReference3 = FileReference.builder()
                    .filename("file3")
                    .partitionId("3")
                    .jobId("job1")
                    .numberOfRecords(100L)
                    .countApproximate(false)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            stateStore.addFile(fileReference3);

            // When
            List<FileReference> fileReferences = stateStore.getFileReferencesWithNoJobId();

            // Then
            assertThat(fileReferences)
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(fileReference1, fileReference2);
        }

        @Test
        void shouldReturnActiveFilesOrderedByPartitionIdThenFilename() throws Exception {
            // Given
            Schema schema = schemaWithKeyAndValueWithTypes(new LongType(), new StringType());
            DynamoDBStateStore stateStore = getStateStore(schema);
            FileReference file1 = FileReference.builder()
                    .filename("file1")
                    .partitionId("P1")
                    .numberOfRecords(100L)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            FileReference file2 = FileReference.builder()
                    .filename("file2")
                    .partitionId("P2")
                    .numberOfRecords(100L)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            FileReference file3 = FileReference.builder()
                    .filename("file3")
                    .partitionId("P1")
                    .numberOfRecords(100L)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            FileReference file4 = FileReference.builder()
                    .filename("file4")
                    .partitionId("P2")
                    .numberOfRecords(100L)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            stateStore.addFiles(List.of(file1, file2, file3, file4));

            // When/Then
            assertThat(stateStore.getFileReferences())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(file1, file3, file2, file4);
        }
    }

    @Nested
    @DisplayName("Split file references")
    class SplitReferences {
        Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
        Schema schema = schemaWithKey("key", new IntType());
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 5)
                .buildTree();
        StateStore store;
        FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(partitions, updateTime);

        @BeforeEach
        void setUp() {
            store = getStateStore(schema);
            store.fixTime(updateTime);
        }

        @Test
        void shouldSucceedIfThereAreOver25SplitRequests() throws Exception {
            // Given
            List<FileReference> fileReferences = new ArrayList<>();
            List<SplitFileReferenceRequest> splitRequests = new ArrayList<>();
            for (int i = 1; i <= 26; i++) {
                FileReference file = factory.rootFile("file" + i, 100L);
                fileReferences.add(file);
                splitRequests.add(splitFileToChildPartitions(file, "L", "R"));
            }
            store.addFiles(fileReferences);

            // When
            store.splitFileReferences(splitRequests);

            // Then
            List<FileReference> expectedReferences = fileReferences.stream()
                    .flatMap(file -> Stream.of(splitFile(file, "L"), splitFile(file, "R")))
                    .collect(toUnmodifiableList());
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(updateTime, expectedReferences));
        }

        @Test
        void shouldThrowExceptionIfThereAre26SplitRequestsAndTheLastOneFails() throws Exception {
            // Given
            List<FileReference> fileReferences = new ArrayList<>();
            List<SplitFileReferenceRequest> splitRequests = new ArrayList<>();
            for (int i = 1; i <= 25; i++) {
                FileReference file = factory.rootFile("file" + i, 100L);
                fileReferences.add(file);
                splitRequests.add(splitFileToChildPartitions(file, "L", "R"));
            }
            splitRequests.add(splitFileToChildPartitions(factory.rootFile("not-found", 100L), "L", "R"));
            store.addFiles(fileReferences);

            // When / Then
            assertThatThrownBy(() -> store.splitFileReferences(splitRequests))
                    .isInstanceOfSatisfying(SplitRequestsFailedException.class, exception ->
                            assertThat(exception)
                                    .extracting(SplitRequestsFailedException::getSuccessfulRequests,
                                            SplitRequestsFailedException::getFailedRequests)
                                    .containsExactly(splitRequests.subList(0, 25), splitRequests.subList(25, 26)))
                    .hasCauseInstanceOf(OneOrMoreFilesNotFoundException.class);
            List<FileReference> expectedReferences = fileReferences.stream()
                    .flatMap(file -> Stream.of(splitFile(file, "L"), splitFile(file, "R")))
                    .collect(toUnmodifiableList());
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(expectedReferences);
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(activeFilesReport(updateTime, expectedReferences));
        }

        @Test
        void shouldThrowExceptionIfTheFirstRequestFails() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);

            // When / Then
            SplitFileReferenceRequest request = splitFileToChildPartitions(file, "L", "R");
            assertThatThrownBy(() ->
                    store.splitFileReferences(List.of(request)))
                    .isInstanceOfSatisfying(SplitRequestsFailedException.class, exception ->
                            assertThat(exception)
                                    .extracting(SplitRequestsFailedException::getSuccessfulRequests,
                                            SplitRequestsFailedException::getFailedRequests)
                                    .containsExactly(List.of(), List.of(request)))
                    .hasCauseInstanceOf(OneOrMoreFilesNotFoundException.class);
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(noFilesReport());
        }

        @Test
        void shouldThrowExceptionIfOneRequestDoesNotFitInATransaction() throws Exception {
            // Given
            FileReference file = factory.rootFile("file", 100L);

            // When / Then
            SplitFileReferenceRequest request = new SplitFileReferenceRequest(file, IntStream.range(0, 100)
                    .mapToObj(i -> SplitFileReference.referenceForChildPartition(file, "" + i, 1))
                    .collect(toUnmodifiableList()));
            assertThatThrownBy(() ->
                    store.splitFileReferences(List.of(request)))
                    .isInstanceOfSatisfying(SplitRequestsFailedException.class, exception ->
                            assertThat(exception)
                                    .extracting(SplitRequestsFailedException::getSuccessfulRequests,
                                            SplitRequestsFailedException::getFailedRequests)
                                    .containsExactly(List.of(), List.of(request)))
                    .hasNoCause();
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getAllFileReferencesWithMaxUnreferenced(100))
                    .isEqualTo(noFilesReport());
        }

        private FileReference splitFile(FileReference parentFile, String childPartitionId) {
            return SplitFileReference.referenceForChildPartition(parentFile, childPartitionId)
                    .toBuilder().lastStateStoreUpdateTime(updateTime).build();
        }
    }

    @Nested
    @DisplayName("Ready for GC files")
    class ReadyForGCFiles {
        @Test
        public void shouldGetFilesThatAreReadyForGC() throws StateStoreException {
            // Given
            Instant file1Time = Instant.parse("2023-06-06T15:00:00Z");
            Instant file2Time = Instant.parse("2023-06-06T15:01:00Z");
            Instant file3Time = Instant.parse("2023-06-06T15:02:00Z");
            Schema schema = schemaWithKeyAndValueWithTypes(new IntType(), new StringType());
            PartitionTree tree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
            DynamoDBStateStore stateStore = getStateStore(schema, tree.getAllPartitions(), 5);
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(tree);
            //  - A file which should be garbage collected immediately
            FileReference fileReference1 = FileReference.builder()
                    .filename("file1")
                    .partitionId("root")
                    .numberOfRecords(100L)
                    .lastStateStoreUpdateTime(file1Time)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            stateStore.fixTime(file1Time);
            stateStore.addFile(fileReference1);
            stateStore.atomicallyAssignJobIdToFileReferences("job1", List.of(fileReference1));
            stateStore.atomicallyReplaceFileReferencesWithNewOne("job1", "root", List.of("file1"),
                    fileReferenceFactory.rootFile("compacted1", 100L));
            //  - An active file which should not be garbage collected
            FileReference fileReference2 = FileReference.builder()
                    .filename("file2")
                    .partitionId("root")
                    .numberOfRecords(100L)
                    .lastStateStoreUpdateTime(file2Time)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            stateStore.fixTime(file2Time);
            stateStore.addFile(fileReference2);
            //  - A file which is ready for garbage collection but which should not be garbage collected now as it has only
            //      just been marked as ready for GC
            FileReference fileReference3 = FileReference.builder()
                    .filename("file3")
                    .partitionId("root")
                    .numberOfRecords(100L)
                    .lastStateStoreUpdateTime(file3Time)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            stateStore.fixTime(file3Time);
            stateStore.addFile(fileReference3);
            stateStore.atomicallyAssignJobIdToFileReferences("job2", List.of(fileReference3));
            stateStore.atomicallyReplaceFileReferencesWithNewOne("job2", "root", List.of("file3"),
                    fileReferenceFactory.rootFile("compacted3", 100L));

            // When / Then 1
            assertThat(stateStore.getReadyForGCFilenamesBefore(file1Time.plus(Duration.ofMinutes(1))))
                    .containsExactly("file1");

            // When / Then 2
            assertThat(stateStore.getReadyForGCFilenamesBefore(file3Time.plus(Duration.ofMinutes(1))))
                    .containsExactly("file1", "file3");
        }

        @Test
        public void shouldDeleteReadyForGCFilename() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore stateStore = getStateStore(schema);
            FileReference fileReference1 = FileReference.builder()
                    .filename("file1")
                    .partitionId("4")
                    .numberOfRecords(100L)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            stateStore.addFile(fileReference1);
            FileReference fileReference2 = FileReference.builder()
                    .filename("file2")
                    .numberOfRecords(100L)
                    .partitionId("5")
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();
            stateStore.atomicallyAssignJobIdToFileReferences("job1", List.of(fileReference1));
            stateStore.atomicallyReplaceFileReferencesWithNewOne("job1", "4", List.of("file1"), fileReference2);

            // When
            stateStore.deleteGarbageCollectedFileReferenceCounts(List.of("file1"));

            // Then
            assertThat(stateStore.getFileReferences())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(fileReference2);
            assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();
        }

        @Test
        public void shouldDeleteMoreThan100ReadyForGCFiles() throws Exception {
            // Given
            Schema schema = schemaWithKey("key");
            Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
            Instant afterUpdateTime = updateTime.plus(Duration.ofMinutes(2));

            List<String> filenames = IntStream.range(0, 101)
                    .mapToObj(i -> "gcFile" + i)
                    .collect(toUnmodifiableList());

            StateStore store = getStateStore(schema);
            store.fixTime(updateTime);
            store.addFilesWithReferences(filenames.stream()
                    .map(AllReferencesToAFileTestHelper::fileWithNoReferences)
                    .collect(toUnmodifiableList()));

            assertThat(store.getReadyForGCFilenamesBefore(afterUpdateTime))
                    .hasSize(101);

            // When / Then
            store.deleteGarbageCollectedFileReferenceCounts(filenames);
            assertThat(store.getFileReferences())
                    .isEmpty();
            assertThat(store.getReadyForGCFilenamesBefore(afterUpdateTime))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Report file status")
    class ReportFileStatus {
        Instant updateTime = Instant.parse("2023-10-04T14:08:00Z");
        Schema schema = schemaWithKey("key", new LongType());
        PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
        StateStore store;
        FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), updateTime);

        @BeforeEach
        void setUp() {
            store = getStateStore(schema);
            store.fixTime(updateTime);
        }

        @Test
        void shouldReportOneActiveFile() throws Exception {
            // Given
            FileReference file = factory.rootFile("test", 100L);
            store.addFile(file);

            // When
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(updateTime, file));
        }

        @Test
        void shouldReportOneReadyForGCFile() throws Exception {
            // Given
            store.addFilesWithReferences(List.of(fileWithNoReferences("test")));

            // When
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(readyForGCFilesReport(updateTime, "test"));
        }

        @Test
        void shouldReportTwoActiveFiles() throws Exception {
            // Given
            FileReference file1 = factory.rootFile("file1", 100L);
            FileReference file2 = factory.rootFile("file2", 100L);
            store.addFiles(List.of(file1, file2));

            // When
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(updateTime, file1, file2));
        }

        @Test
        void shouldReportFileSplitOverTwoPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            store.addFiles(List.of(leftFile, rightFile));

            // When
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(updateTime, leftFile, rightFile));
        }

        @Test
        void shouldReportFileSplitOverTwoPartitionsWithOneSideCompacted() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            FileReference rootFile = factory.rootFile("file", 100L);
            FileReference leftFile = splitFile(rootFile, "L");
            FileReference rightFile = splitFile(rootFile, "R");
            FileReference outputFile = factory.partitionFile("L", "outputFile", 100L);
            store.addFiles(List.of(leftFile, rightFile));
            store.atomicallyAssignJobIdToFileReferences("job1", List.of(leftFile));
            store.atomicallyReplaceFileReferencesWithNewOne("job1", "L", List.of("file"), outputFile);

            // When
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(5);

            // Then
            assertThat(report).isEqualTo(activeFilesReport(updateTime, rightFile, outputFile));
        }

        @Test
        void shouldReportReadyForGCFilesWithLimit() throws Exception {
            // Given
            store.addFilesWithReferences(List.of(
                    fileWithNoReferences("test1"),
                    fileWithNoReferences("test2"),
                    fileWithNoReferences("test3")));

            // When
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(2);

            // Then
            assertThat(report).isEqualTo(partialReadyForGCFilesReport(updateTime, "test1", "test2"));
        }

        @Test
        void shouldReportReadyForGCFilesMeetingLimit() throws Exception {
            // Given
            store.addFilesWithReferences(List.of(
                    fileWithNoReferences("test1"),
                    fileWithNoReferences("test2")));

            // When
            AllReferencesToAllFiles report = store.getAllFileReferencesWithMaxUnreferenced(2);

            // Then
            assertThat(report).isEqualTo(readyForGCFilesReport(updateTime, "test1", "test2"));
        }

        private void splitPartition(String parentId, String leftId, String rightId, long splitPoint) {
            partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint);
            factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), updateTime);
        }

        private FileReference splitFile(FileReference parentFile, String childPartitionId) {
            return SplitFileReference.referenceForChildPartition(parentFile, childPartitionId)
                    .toBuilder().lastStateStoreUpdateTime(updateTime).build();
        }
    }

    @Nested
    @DisplayName("Atomically update files")
    class AtomicallyUpdateFiles {
        @Test
        public void shouldAtomicallyReplaceFileReferencesWithNewOne() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore stateStore = getStateStore(schema);
            List<String> filesToMoveToReadyForGC = new ArrayList<>();
            List<FileReference> fileReferencesToMoveToReadyForGC = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileReference fileReference = FileReference.builder()
                        .filename("file" + i)
                        .partitionId("7")
                        .numberOfRecords(100L)
                        .countApproximate(true)
                        .onlyContainsDataForThisPartition(false)
                        .build();
                fileReferencesToMoveToReadyForGC.add(fileReference);
                filesToMoveToReadyForGC.add(fileReference.getFilename());
                stateStore.addFile(fileReference);
            }
            FileReference newFileReference = FileReference.builder()
                    .filename("file-new")
                    .partitionId("7")
                    .numberOfRecords(100L)
                    .countApproximate(true)
                    .onlyContainsDataForThisPartition(false)
                    .build();

            // When
            stateStore.atomicallyAssignJobIdToFileReferences("job1", fileReferencesToMoveToReadyForGC);
            stateStore.atomicallyReplaceFileReferencesWithNewOne("job1", "7", filesToMoveToReadyForGC, newFileReference);

            // Then
            assertThat(stateStore.getFileReferences())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(newFileReference);
            assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                    .containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
        }

        @Test
        public void atomicallyReplaceFileReferencesWithNewOneShouldFailIfJobHasAlreadyBeenApplied() throws StateStoreException {
            // Given two input files have been compacted into one
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
            StateStore stateStore = getStateStore(schema, partitions.getAllPartitions());
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(partitions);
            FileReference inputFile1 = fileReferenceFactory.rootFile("inputFile1", 100L);
            FileReference inputFile2 = fileReferenceFactory.rootFile("inputFile2", 100L);
            FileReference outputFile = fileReferenceFactory.rootFile("outputFile", 200L);
            stateStore.addFiles(List.of(inputFile1, inputFile2));
            stateStore.atomicallyAssignJobIdToFileReferences("job1", List.of(inputFile1, inputFile2));
            stateStore.atomicallyReplaceFileReferencesWithNewOne("job1", "root", List.of("inputFile1", "inputFile2"), outputFile);

            // When we attempt to re-apply the compaction
            // Then it fails
            assertThatThrownBy(() ->
                    stateStore.atomicallyReplaceFileReferencesWithNewOne("job1", "root", List.of("inputFile1", "inputFile2"), outputFile))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldAtomicallyUpdateJobStatusOfFiles() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore stateStore = getStateStore(schema);
            List<FileReference> files = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileReference fileReference = FileReference.builder()
                        .filename("file" + i)
                        .partitionId("8")
                        .numberOfRecords(100L)
                        .countApproximate(true)
                        .onlyContainsDataForThisPartition(false)
                        .build();
                files.add(fileReference);
                stateStore.addFile(fileReference);
            }
            String jobId = UUID.randomUUID().toString();

            // When
            stateStore.atomicallyAssignJobIdToFileReferences(jobId, files);

            // Then
            assertThat(stateStore.getFileReferences())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("jobId", "lastStateStoreUpdateTime")
                    .containsExactlyInAnyOrderElementsOf(files)
                    .extracting(FileReference::getJobId).containsOnly(jobId);
            assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();
        }

        @Test
        public void shouldNotAtomicallyCreateJobAndUpdateJobStatusOfFilesWhenJobIdAlreadySet() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore stateStore = getStateStore(schema);
            List<FileReference> files = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileReference fileReference = FileReference.builder()
                        .filename("file" + i)
                        .partitionId("9")
                        .numberOfRecords(100L)
                        .jobId("compactionJob")
                        .countApproximate(true)
                        .onlyContainsDataForThisPartition(false)
                        .build();
                files.add(fileReference);
                stateStore.addFile(fileReference);
            }
            String jobId = UUID.randomUUID().toString();

            // When / Then
            assertThatThrownBy(() ->
                    stateStore.atomicallyAssignJobIdToFileReferences(jobId, files))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldNotAtomicallyUpdateJobStatusOfFilesIfFileReferenceNotPresent() {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore stateStore = getStateStore(schema);
            List<FileReference> files = new ArrayList<>();
            for (int i = 1; i < 5; i++) {
                FileReference fileReference = FileReference.builder()
                        .filename("file" + i)
                        .partitionId("8")
                        .numberOfRecords(1000L)
                        .countApproximate(true)
                        .onlyContainsDataForThisPartition(false)
                        .build();
                files.add(fileReference);
            }
            String jobId = UUID.randomUUID().toString();

            // When / Then
            assertThatThrownBy(() -> stateStore.atomicallyAssignJobIdToFileReferences(jobId, files))
                    .isInstanceOf(StateStoreException.class);
        }
    }

    @Nested
    @DisplayName("Initialise partitions")
    class InitialisePartitions {

        @Test
        public void shouldCorrectlyInitialisePartitionsWithLongKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList(100L))
                    .construct();
            StateStore stateStore = getStateStore(schema, partitions);

            // When / Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
        }

        @Test
        public void shouldCorrectlyInitialisePartitionsWithStringKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new StringType());
            List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList("B"))
                    .construct();
            StateStore stateStore = getStateStore(schema, partitions);

            // When / Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
        }

        @Test
        public void shouldCorrectlyInitialisePartitionsWithByteArrayKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
            byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
            byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
            List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                    .construct();
            StateStore stateStore = getStateStore(schema, partitions);

            // When / Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
        }

        @Test
        public void shouldCorrectlyInitialisePartitionsWithMultidimensionalKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
            byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
            byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
            List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                    .construct();
            StateStore stateStore = getStateStore(schema, partitions);

            // When / Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
        }

        @Test
        public void shouldCorrectlyStoreNonLeafPartitionWithByteArrayKeyType() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
            byte[] min = new byte[]{1, 2, 3, 4};
            byte[] max = new byte[]{5, 6, 7, 8, 9};
            Range range = new RangeFactory(schema).createRange("key", min, max);
            Partition partition = Partition.builder()
                    .rowKeyTypes(schema.getRowKeyTypes())
                    .region(new Region(range))
                    .id("id")
                    .leafPartition(false)
                    .parentPartitionId("P")
                    .childPartitionIds(new ArrayList<>())
                    .dimension(0)
                    .build();
            StateStore stateStore = getStateStore(schema, Collections.singletonList(partition));

            // When
            Partition retrievedPartition = stateStore.getAllPartitions().get(0);

            // Then
            assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key").getMin());
            assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key").getMax());
            assertThat(retrievedPartition.getId()).isEqualTo(partition.getId());
            assertThat(retrievedPartition.getParentPartitionId()).isEqualTo(partition.getParentPartitionId());
            assertThat(retrievedPartition.getChildPartitionIds()).isEqualTo(partition.getChildPartitionIds());
            assertThat(retrievedPartition.getDimension()).isEqualTo(partition.getDimension());
        }

        // TODO shouldCorrectlyStorePartitionWithMultidimensionalKeyType

        @Test
        public void shouldReturnCorrectPartitionToFileMapping() throws StateStoreException {
            // Given
            Schema schema = schemaWithSingleRowKeyType(new LongType());
            StateStore stateStore = getStateStore(schema);
            List<FileReference> files = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                FileReference fileReference = FileReference.builder()
                        .filename("file" + i)
                        .partitionId("" + (i % 5))
                        .numberOfRecords(100L)
                        .countApproximate(true)
                        .onlyContainsDataForThisPartition(false)
                        .build();
                files.add(fileReference);
                stateStore.addFile(fileReference);
            }

            // When
            Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToReferencedFilesMap();

            // Then
            assertThat(partitionToFileMapping.entrySet()).hasSize(5);
            for (int i = 0; i < 5; i++) {
                assertThat(partitionToFileMapping.get("" + i)).hasSize(2);
                Set<String> expected = new HashSet<>();
                expected.add(files.get(i).getFilename());
                expected.add(files.get(i + 5).getFilename());
                assertThat(new HashSet<>(partitionToFileMapping.get("" + i))).isEqualTo(expected);
            }
        }

        @Test
        public void shouldReturnAllPartitions() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();

            PartitionTree tree = new PartitionsBuilder(schema).rootFirst("root")
                    .splitToNewChildren("root", "left", "right", 100L)
                    .splitToNewChildren("left", "id1", "id2", 1L)
                    .splitToNewChildren("right", "id3", "id4", 200L).buildTree();

            StateStore stateStore = getStateStore(schema, tree.getAllPartitions());

            // When / Then
            assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }

        @Test
        public void shouldReturnLeafPartitions() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();

            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .buildTree();

            PartitionTree intermediateTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "id1", "id2", 1L)
                    .buildTree();

            PartitionTree expectedTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "id1", "id2", 1L)
                    .splitToNewChildren("id2", "id3", "id4", 9L).buildTree();
            DynamoDBStateStore stateStore = getStateStore(schema, tree.getAllPartitions());

            // When
            stateStore.atomicallyUpdatePartitionAndCreateNewOnes(intermediateTree.getRootPartition(), intermediateTree.getPartition("id1"), intermediateTree.getPartition("id2"));
            stateStore.atomicallyUpdatePartitionAndCreateNewOnes(expectedTree.getPartition("id2"), expectedTree.getPartition("id3"), expectedTree.getPartition("id4"));

            // Then
            assertThat(stateStore.getLeafPartitions())
                    .containsExactlyInAnyOrderElementsOf(expectedTree.getAllPartitions().stream().filter(Partition::isLeafPartition).collect(Collectors.toList()));
        }

        @Test
        public void shouldUpdatePartitions() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore stateStore = getStateStore(schema);

            // When
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "child1", "child2", 0L)
                    .buildTree();

            stateStore.atomicallyUpdatePartitionAndCreateNewOnes(tree.getRootPartition(), tree.getPartition("child1"), tree.getPartition("child2"));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }

        @Test
        public void shouldNotUpdatePartitionsIfLeafStatusChanges() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore stateStore = getStateStore(schema);

            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "child1", "child2", 0L)
                    .buildTree();

            stateStore.initialise(tree.getAllPartitions());

            // When / Then
            //  - Attempting to split something that has already been split should fail
            assertThatThrownBy(() ->
                    stateStore.atomicallyUpdatePartitionAndCreateNewOnes(tree.getPartition("root"), tree.getPartition("child1"), tree.getPartition("child2")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldThrowExceptionWithPartitionSplitRequestWhereParentIsLeaf() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            StateStore stateStore = getStateStore(schema);
            Partition parentPartition = stateStore.getAllPartitions().get(0);
            parentPartition = parentPartition.toBuilder().childPartitionIds(Arrays.asList("child1", "child2")).build();
            Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition1 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child1")
                    .region(region1)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();
            Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition2 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child2")
                    .region(region2)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();

            // When / Then
            Partition finalParentPartition = parentPartition;
            assertThatThrownBy(() ->
                    stateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldThrowExceptionWithPartitionSplitRequestWhereChildrenWrong() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            StateStore stateStore = getStateStore(schema);
            Partition parentPartition = stateStore.getAllPartitions().get(0);
            parentPartition = parentPartition.toBuilder()
                    .leafPartition(false)
                    .childPartitionIds(Arrays.asList("child3", "child2")) // Wrong children
                    .build();
            Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition1 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child1")
                    .region(region1)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();
            Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition2 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child2")
                    .region(region2)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();

            // When / Then
            Partition finalParentPartition = parentPartition;
            assertThatThrownBy(() ->
                    stateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldThrowExceptionWithPartitionSplitRequestWhereParentWrong() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            StateStore stateStore = getStateStore(schema);
            Partition parentPartition = stateStore.getAllPartitions().get(0);
            parentPartition = parentPartition.toBuilder()
                    .leafPartition(false)
                    .childPartitionIds(Arrays.asList("child1", "child2"))
                    .build();
            Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition1 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child1")
                    .region(region1)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("notparent") // Wrong parent
                    .build();
            Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
            Partition childPartition2 = Partition.builder()
                    .rowKeyTypes(new LongType())
                    .leafPartition(true)
                    .id("child2")
                    .region(region2)
                    .childPartitionIds(new ArrayList<>())
                    .parentPartitionId("parent")
                    .build();

            // When / Then
            Partition finalParentPartition = parentPartition;
            assertThatThrownBy(() ->
                    stateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldInitialiseRootPartitionCorrectlyForIntKey() throws StateStoreException {
            // Given
            Field field = new Field("key", new IntType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore stateStore = getStateStore(schema);

            // When
            List<Partition> partitions = stateStore.getAllPartitions();

            // Then
            Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());
            assertThat(partitions).containsExactly(expectedPartition);
        }

        @Test
        public void shouldInitialiseRootPartitionCorrectlyForLongKey() throws StateStoreException {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore stateStore = getStateStore(schema);

            // When
            List<Partition> partitions = stateStore.getAllPartitions();

            // Then
            Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());
            assertThat(partitions).containsExactly(expectedPartition);
        }

        @Test
        public void shouldInitialiseRootPartitionCorrectlyForStringKey() throws StateStoreException {
            // Given
            Field field = new Field("key", new StringType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore stateStore = getStateStore(schema);

            // When
            List<Partition> partitions = stateStore.getAllPartitions();

            // Then
            Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());
            assertThat(partitions).containsExactly(expectedPartition);
        }

        @Test
        public void shouldInitialiseRootPartitionCorrectlyForByteArrayKey() throws StateStoreException {
            // Given
            Field field = new Field("key", new ByteArrayType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            StateStore stateStore = getStateStore(schema);

            // When
            List<Partition> partitions = stateStore.getAllPartitions();
            Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());

            // Then
            assertThat(partitions).containsExactly(expectedPartition);
        }

        @Test
        void shouldNotReinitialisePartitionsWhenAFileIsPresent() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            PartitionTree treeBefore = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "before1", "before2", 0L)
                    .buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "after1", "after2", 10L)
                    .buildTree();
            StateStore stateStore = getStateStore(schema, treeBefore.getAllPartitions());
            stateStore.addFile(FileReferenceFactory.from(treeBefore).partitionFile("before2", 100L));

            // When / Then
            assertThatThrownBy(() -> stateStore.initialise(treeAfter.getAllPartitions()))
                    .isInstanceOf(StateStoreException.class);
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(treeBefore.getAllPartitions());
        }

        @Test
        public void shouldReinitialisePartitionsWhenNoFilesArePresent() throws StateStoreException {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            PartitionTree treeBefore = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "before1", "before2", 0L)
                    .buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "after1", "after2", 10L)
                    .buildTree();
            StateStore stateStore = getStateStore(schema, treeBefore.getAllPartitions());

            // When
            stateStore.initialise(treeAfter.getAllPartitions());

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(treeAfter.getAllPartitions());
        }
    }

    private TableProperties createTable(Schema schema, int garbageCollectorDelayBeforeDeletionInMinutes) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, garbageCollectorDelayBeforeDeletionInMinutes);
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBStateStore.class.getName());
        return tableProperties;
    }

    private DynamoDBStateStore getStateStore(Schema schema,
                                             List<Partition> partitions,
                                             int garbageCollectorDelayBeforeDeletionInMinutes) {
        TableProperties tableProperties = createTable(schema, garbageCollectorDelayBeforeDeletionInMinutes);
        DynamoDBStateStore stateStore = new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDBClient);
        try {
            stateStore.initialise(partitions);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        return stateStore;
    }

    private DynamoDBStateStore getStateStore(Schema schema,
                                             List<Partition> partitions) {
        return getStateStore(schema, partitions, 0);
    }

    private DynamoDBStateStore getStateStore(Schema schema, int garbageCollectorDelayBeforeDeletionInMinutes) {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct(), garbageCollectorDelayBeforeDeletionInMinutes);
    }

    private DynamoDBStateStore getStateStore(Schema schema) {
        return getStateStore(schema, 0);
    }

    private Schema schemaWithSingleRowKeyType(PrimitiveType type) {
        return Schema.builder().rowKeyFields(new Field("key", type)).build();
    }

    private Schema schemaWithTwoRowKeyTypes(PrimitiveType type1, PrimitiveType type2) {
        return Schema.builder().rowKeyFields(new Field("key1", type1), new Field("key2", type2)).build();
    }

    private Schema schemaWithKeyAndValueWithTypes(PrimitiveType keyType, Type valueType) {
        return Schema.builder()
                .rowKeyFields(new Field("key", keyType))
                .valueFields(new Field("value", valueType))
                .build();
    }
}
