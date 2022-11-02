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
package sleeper.statestore.inmemory;

import org.junit.Test;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.PartitionStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FixedPartitionStoreTest {

    @Test
    public void shouldInitialiseStoreWithSinglePartition() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionStore store = new FixedPartitionStore(schema);

        // When
        store.initialise();
        PartitionTree tree = new PartitionTree(schema, store.getAllPartitions());
        Partition root = tree.getRootPartition();

        // Then
        assertThat(store.getAllPartitions()).containsExactly(root);
        assertThat(store.getLeafPartitions()).containsExactly(root);
        assertThat(root.getChildPartitionIds()).isEmpty();
    }

    @Test
    public void shouldInitialiseStoreWithPartitionTree() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionStore store = new FixedPartitionStore(schema);
        List<Partition> partitions = new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList("aaa"))
                .parentJoining("C", "A", "B")
                .buildList();
        PartitionTree tree = new PartitionTree(schema, partitions);

        // When
        store.initialise(partitions);

        // Then
        assertThat(store.getAllPartitions()).containsExactlyInAnyOrder(
                tree.getPartition("A"), tree.getPartition("B"), tree.getPartition("C"));
        assertThat(store.getLeafPartitions()).containsExactlyInAnyOrder(
                tree.getPartition("A"), tree.getPartition("B"));
    }

    @Test
    public void shouldRefusePartitionSplit() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        List<Partition> partitionsInit = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildList();
        PartitionStore store = new FixedPartitionStore(schema);
        store.initialise(partitionsInit);

        // When / Then
        PartitionTree updateTree = new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("left", "right"), Collections.singletonList("aaa"))
                .parentJoining("root", "left", "right")
                .buildTree();
        Partition root = updateTree.getPartition("root");
        Partition left = updateTree.getPartition("left");
        Partition right = updateTree.getPartition("right");
        assertThatThrownBy(() -> store.atomicallyUpdatePartitionAndCreateNewOnes(root, left, right))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void shouldRefuseInitialiseWhenAlreadyInitialised() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        List<Partition> partitionsInit = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildList();
        PartitionStore store = new FixedPartitionStore(schema);
        store.initialise(partitionsInit);

        // When / Then
        List<Partition> partitionsUpdate = new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("left", "right"), Collections.singletonList("aaa"))
                .parentJoining("root", "left", "right")
                .buildList();
        assertThatThrownBy(() -> store.initialise(partitionsUpdate))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
