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
package sleeper.core.statestore.transactionlog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.fixJitterSeed;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.recordWaits;

public class TransactionLogStateStoreLogSpecificTest {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    private final InMemoryTransactionLogStore filesLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogStore partitionsLogStore = new InMemoryTransactionLogStore();
    private final List<Duration> retryWaits = new ArrayList<>();
    private StateStore store = stateStore();

    @BeforeEach
    void setUp() throws Exception {
        store.initialise(partitions.buildList());
    }

    @Test
    void shouldAddTransactionWhenAnotherProcessAddedATransactionBetweenAdds() throws Exception {
        // Given
        PartitionTree afterRootSplit = partitions.splitToNewChildren("root", "L", "R", "l").buildTree();
        otherProcess().atomicallyUpdatePartitionAndCreateNewOnes(
                afterRootSplit.getPartition("root"),
                afterRootSplit.getPartition("L"), afterRootSplit.getPartition("R"));

        // When
        PartitionTree afterLeftSplit = partitions.splitToNewChildren("L", "LL", "LR", "f").buildTree();
        store.atomicallyUpdatePartitionAndCreateNewOnes(
                afterLeftSplit.getPartition("L"),
                afterLeftSplit.getPartition("LL"), afterLeftSplit.getPartition("LR"));

        // Then
        assertThat(new PartitionTree(store.getAllPartitions()))
                .isEqualTo(afterLeftSplit);
        assertThat(retryWaits).isEmpty();
    }

    @Test
    void shouldRetryAddTransactionWhenAnotherProcessAddedATransactionBetweenUpdateAndAdd() throws Exception {
        // Given
        FileReference file1 = fileFactory().rootFile("file1.parquet", 100);
        FileReference file2 = fileFactory().rootFile("file2.parquet", 200);
        FileReference file3 = fileFactory().rootFile("file3.parquet", 300);
        store.addFile(file1);
        filesLogStore.beforeNextAddTransaction(() -> {
            otherProcess().addFile(file2);
        });

        // When
        store.addFile(file3);

        // Then
        assertThat(store.getFileReferences())
                .containsExactly(file1, file2, file3);
        assertThat(retryWaits).hasSize(1);
    }

    @Test
    void shouldRetryAddTransactionWhenUnexpectedFailureOccurredAddingTransaction() throws Exception {
        // Given
        filesLogStore.beforeNextAddTransaction(() -> {
            throw new RuntimeException("Unexpected failure");
        });
        FileReference file = fileFactory().rootFile("file.parquet", 100);

        // When
        store.addFile(file);

        // Then
        assertThat(store.getFileReferences())
                .containsExactly(file);
        assertThat(retryWaits).hasSize(1);
    }

    @Test
    void shouldFailAfterTooManyTries() throws Exception {
        // Given
        store = stateStore(builder -> builder.maxAddTransactionAttempts(1));
        RuntimeException failure = new RuntimeException("Unexpected failure");
        filesLogStore.beforeNextAddTransaction(() -> {
            throw failure;
        });
        FileReference file = fileFactory().rootFile("file.parquet", 100);

        // When / Then
        assertThatThrownBy(() -> store.addFile(file))
                .isInstanceOf(StateStoreException.class)
                .hasCause(failure);
        assertThat(store.getFileReferences())
                .isEmpty();
        assertThat(retryWaits).isEmpty();
    }

    @Test
    void shouldFailIfNoAttemptsConfigured() throws Exception {
        // Given
        store = stateStore(builder -> builder.maxAddTransactionAttempts(0));
        FileReference file = fileFactory().rootFile("file.parquet", 100);

        // When / Then
        assertThatThrownBy(() -> store.addFile(file))
                .isInstanceOf(StateStoreException.class)
                .cause().isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No attempts made");
        assertThat(store.getFileReferences())
                .isEmpty();
        assertThat(retryWaits).isEmpty();
    }

    private StateStore otherProcess() {
        return stateStore();
    }

    private StateStore stateStore() {
        return stateStore(builder -> {
        });
    }

    private StateStore stateStore(Consumer<TransactionLogStateStore.Builder> config) {
        TransactionLogStateStore.Builder builder = TransactionLogStateStore.builder()
                .schema(schema)
                .filesLogStore(filesLogStore)
                .partitionsLogStore(partitionsLogStore)
                .maxAddTransactionAttempts(10)
                .retryBackoff(new ExponentialBackoffWithJitter(
                        WaitRange.firstAndMaxWaitCeilingSecs(1, 30),
                        fixJitterSeed(), recordWaits(retryWaits)));
        config.accept(builder);
        StateStore stateStore = builder.build();
        stateStore.fixTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }
}
