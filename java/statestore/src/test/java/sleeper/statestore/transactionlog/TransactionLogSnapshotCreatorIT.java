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
package sleeper.statestore.transactionlog;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotMetadataStore.LatestSnapshots;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.SnapshotMetadataSaver;

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

public class TransactionLogSnapshotCreatorIT extends TransactionLogSnapshotTestBase {

    @Test
    void shouldCreateSnapshotsForOneTable() throws Exception {
        // Given we create a transaction log in memory
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 123L)
                .buildTree();
        FileReference file = FileReferenceFactory.fromUpdatedAt(partitions, DEFAULT_UPDATE_TIME).rootFile(123);
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore inMemoryStateStore = createStateStoreWithInMemoryTransactionLog(table);
        inMemoryStateStore.initialise(partitions.getAllPartitions());
        inMemoryStateStore.addFile(file);

        // When we create a snapshot from the in-memory transactions
        createSnapshots(table);

        // Then when we read from a state store with no transaction log, we load the state from the snapshot
        StateStore stateStore = createStateStore(table);
        assertThat(stateStore.getAllPartitions()).isEqualTo(partitions.getAllPartitions());
        assertThat(stateStore.getFileReferences()).containsExactly(file);
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 1),
                        partitionsSnapshot(table, 1)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 1));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 1));
    }

    @Test
    void shouldCreateSnapshotsForMultipleTables() throws Exception {
        // Given
        TableProperties table1 = createTable("test-table-id-1", "test-table-1");
        PartitionTree partitions1 = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "A", "B", 123L)
                .buildTree();
        FileReference file1 = FileReferenceFactory.fromUpdatedAt(partitions1, DEFAULT_UPDATE_TIME)
                .rootFile("file1.parquet", 123L);
        StateStore inMemoryStateStore1 = createStateStoreWithInMemoryTransactionLog(table1);
        inMemoryStateStore1.initialise(partitions1.getAllPartitions());
        inMemoryStateStore1.addFile(file1);

        TableProperties table2 = createTable("test-table-id-2", "test-table-2");
        PartitionTree partitions2 = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "C", "D", 123L)
                .buildTree();
        FileReference file2 = FileReferenceFactory.fromUpdatedAt(partitions2, DEFAULT_UPDATE_TIME)
                .rootFile("file2.parquet", 123L);
        StateStore inMemoryStateStore2 = createStateStoreWithInMemoryTransactionLog(table2);
        inMemoryStateStore2.initialise(partitions2.getAllPartitions());
        inMemoryStateStore2.addFile(file2);

        // When
        createSnapshots(table1);
        createSnapshots(table2);

        // Then
        StateStore stateStore1 = createStateStore(table1);
        assertThat(stateStore1.getAllPartitions()).isEqualTo(partitions1.getAllPartitions());
        assertThat(stateStore1.getFileReferences()).containsExactly(file1);
        assertThat(snapshotStore(table1).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table1, 1),
                        partitionsSnapshot(table1, 1)));
        assertThat(snapshotStore(table1).getFilesSnapshots())
                .containsExactly(filesSnapshot(table1, 1));
        assertThat(snapshotStore(table1).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table1, 1));

        StateStore stateStore2 = createStateStore(table2);
        assertThat(stateStore2.getAllPartitions()).isEqualTo(partitions2.getAllPartitions());
        assertThat(stateStore2.getFileReferences()).containsExactly(file2);
        assertThat(snapshotStore(table2).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table2, 1),
                        partitionsSnapshot(table2, 1)));
        assertThat(snapshotStore(table2).getFilesSnapshots())
                .containsExactly(filesSnapshot(table2, 1));
        assertThat(snapshotStore(table2).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table2, 1));
    }

    @Test
    void shouldCreateMultipleSnapshotsForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();
        FileReference file1 = FileReferenceFactory.from(partitions.buildTree())
                .rootFile(123L);
        stateStore.addFile(file1);
        createSnapshots(table);

        // When
        partitions.splitToNewChildren("root", "L", "R", 123L)
                .applySplit(stateStore, "root");
        FileReference file2 = FileReferenceFactory.from(partitions.buildTree())
                .partitionFile("L", 456L);
        stateStore.addFile(file2);
        createSnapshots(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 2),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(
                        filesSnapshot(table, 1),
                        filesSnapshot(table, 2));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(
                        partitionsSnapshot(table, 1),
                        partitionsSnapshot(table, 2));
    }

    @Test
    void shouldSkipCreatingSnapshotsIfStateHasNotUpdatedSinceLastSnapshot() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile(123L));
        createSnapshots(table);

        // When
        createSnapshots(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 1),
                        partitionsSnapshot(table, 1)));
    }

    @Test
    void shouldNotCreateSnapshotForTableWithNoTransactions() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");

        // When
        createSnapshots(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(LatestSnapshots.empty());
        assertThat(snapshotStore(table).getFilesSnapshots()).isEmpty();
        assertThat(snapshotStore(table).getPartitionsSnapshots()).isEmpty();
    }

    @Test
    void shouldNotCreateFileSnapshotForTableWithOnlyPartitionTransactions() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();

        // When
        createSnapshots(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(null, partitionsSnapshot(table, 1)));
        assertThat(snapshotStore(table).getFilesSnapshots()).isEmpty();
        assertThat(snapshotStore(table).getPartitionsSnapshots()).containsExactly(partitionsSnapshot(table, 1));
    }

    @Test
    void shouldRemoveFilesSnapshotFileIfDynamoTransactionFailed() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();
        createSnapshots(table);
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile(123L));

        // When / Then
        IllegalStateException exception = new IllegalStateException();
        assertThatThrownBy(() -> createSnapshots(table, failedUpdate(exception)))
                .isSameAs(exception);
        assertThat(snapshotStore(table).getFilesSnapshots()).isEmpty();
        assertThat(filesSnapshotFileExists(table, 1)).isFalse();
    }

    @Test
    void shouldRemovePartitionsSnapshotFileIfDynamoTransactionFailed() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();

        // When / Then
        IllegalStateException exception = new IllegalStateException();
        assertThatThrownBy(() -> createSnapshots(table, failedUpdate(exception)))
                .isSameAs(exception);
        assertThat(snapshotStore(table).getPartitionsSnapshots()).isEmpty();
        assertThat(partitionsSnapshotFileExists(table, 1)).isFalse();
    }

    @Test
    void shouldNotCreateSnapshotIfLoadingPreviousPartitionSnapshotFails() throws Exception {
        // Given we delete the partitions file for the last snapshot
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise(partitions.buildList());
        createSnapshots(table);
        TransactionLogSnapshotMetadata snapshot = getLatestPartitionsSnapshot(table);
        deleteSnapshotFile(snapshot);
        // And we add a transaction that would trigger a new snapshot
        partitions.splitToNewChildren("root", "L", "R", 123L)
                .applySplit(stateStore, "root");

        // When / Then
        assertThatThrownBy(() -> createSnapshots(table))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(FileNotFoundException.class);
        assertThat(snapshotStore(table).getPartitionsSnapshots()).containsExactly(snapshot);
    }

    @Test
    void shouldNotCreateSnapshotIfLoadingPreviousFileSnapshotFails() throws Exception {
        // Given we delete the files file for the last snapshot
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();
        stateStore.addFile(FileReferenceFactory.from(stateStore).rootFile("file1.parquet", 123));
        createSnapshots(table);
        TransactionLogSnapshotMetadata snapshot = getLatestFilesSnapshot(table);
        deleteSnapshotFile(snapshot);
        // And we add a transaction that would trigger a new snapshot
        stateStore.addFile(FileReferenceFactory.from(stateStore).rootFile("file2.parquet", 456));

        // When / Then
        assertThatThrownBy(() -> createSnapshots(table))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(FileNotFoundException.class);
        assertThat(snapshotStore(table).getFilesSnapshots()).containsExactly(snapshot);
    }

    private StateStore createStateStore(TableProperties tableProperties) {
        StateStore stateStore = DynamoDBTransactionLogStateStore.create(instanceProperties, tableProperties, dynamoDBClient, s3Client, configuration);
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }

    private SnapshotMetadataSaver failedUpdate(RuntimeException exception) {
        return snapshot -> {
            throw exception;
        };
    }
}
