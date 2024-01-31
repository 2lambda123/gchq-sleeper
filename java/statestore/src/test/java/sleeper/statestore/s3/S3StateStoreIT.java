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
package sleeper.statestore.s3;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class S3StateStoreIT extends S3StateStoreTestBase {
    protected final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    @Test
    public void shouldReturnCorrectPartitionToFileMapping() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore stateStore = getStateStore(schema);
        List<FileReference> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FileReference fileReference = FileReference.builder()
                    .filename("file" + i)
                    .partitionId("" + (i % 5))
                    .numberOfRecords((long) i)
                    .countApproximate(false)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            files.add(fileReference);
        }
        stateStore.addFiles(files);

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
    public void shouldReturnAllPartitions() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 100L)
                .splitToNewChildren("left", "id0", "id2", 1L)
                .splitToNewChildren("right", "id1", "id3", 200L)
                .buildTree();
        S3StateStore stateStore = getStateStore(schema, tree.getAllPartitions());
        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
    }

    @Test
    public void shouldReturnLeafPartitionsAfterPartitionUpdate() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildTree();
        S3StateStore stateStore = getStateStore(schema, List.of(tree.getRootPartition()));
        PartitionTree stepOneTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "id1", "id2", 1L)
                .buildTree();

        PartitionTree expectedTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "id1", "id2", 1L)
                .splitToNewChildren("id2", "id3", "id4", 9L)
                .buildTree();

        // When
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(stepOneTree.getRootPartition(), stepOneTree.getPartition("id1"), stepOneTree.getPartition("id2"));
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(expectedTree.getPartition("id2"), expectedTree.getPartition("id3"), expectedTree.getPartition("id4"));

        // Then
        assertThat(stateStore.getLeafPartitions())
                .containsExactlyInAnyOrderElementsOf(expectedTree.getAllPartitions().stream().filter(Partition::isLeafPartition).collect(Collectors.toList()));
    }

    @Test
    public void shouldUpdatePartitions() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildTree();
        S3StateStore stateStore = getStateStore(schema, tree.getAllPartitions());

        // When
        PartitionTree expectedTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "child1", "child2", 0L)
                .buildTree();
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(expectedTree.getRootPartition(), expectedTree.getPartition("child1"), expectedTree.getPartition("child2"));

        // Then
        assertThat(stateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(expectedTree.getAllPartitions());
    }

    @Test
    public void shouldNotUpdatePartitionsIfLeafStatusChanges() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "child1", "child2", 0L)
                .buildTree();

        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(tree.getPartition("root"), tree.getPartition("child1"), tree.getPartition("child2"));

        // When / Then
        //  - Attempting to split something that has already been split should fail
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                        tree.getPartition("root"), tree.getPartition("child1"), tree.getPartition("child2")))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereChildrenWrong() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        Partition parentPartitionAfterSplit = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child3", "child2")) // Wrong children
                .build();
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                        parentPartitionAfterSplit, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentWrong() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        Partition parentPartitionAfterSplit = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child1", "child2"))
                .build();
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("notparent") // Wrong parent
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                        parentPartitionAfterSplit, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereNewPartitionIsNotLeaf() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        Partition parentPartitionAfterSplit = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child1", "child2"))
                .build();
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 0L));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 0L, Long.MAX_VALUE));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(false) // Not leaf
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                        parentPartitionAfterSplit, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForIntKey() throws Exception {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Partition expectedPartition = new PartitionsBuilder(schema)
                .rootFirst(partitions.get(0).getId())
                .buildTree()
                .getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForLongKey() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Partition expectedPartition = new PartitionsBuilder(schema)
                .rootFirst(partitions.get(0).getId())
                .buildTree()
                .getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForStringKey() throws Exception {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Partition expectedPartition = new PartitionsBuilder(schema)
                .rootFirst(partitions.get(0).getId())
                .buildTree()
                .getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForByteArrayKey() throws Exception {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Partition expectedPartition = new PartitionsBuilder(schema)
                .rootFirst(partitions.get(0).getId())
                .buildTree()
                .getPartition(partitions.get(0).getId());
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
    void shouldReinitialisePartitionsWhenNoFilesArePresent() throws Exception {
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

    private S3StateStore getStateStore(Schema schema,
                                       List<Partition> partitions,
                                       int garbageCollectorDelayBeforeDeletionInMinutes) throws StateStoreException {
        tableProperties.setSchema(schema);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, garbageCollectorDelayBeforeDeletionInMinutes);
        S3StateStore stateStore = new S3StateStore(instanceProperties, tableProperties, dynamoDBClient, new Configuration());
        stateStore.initialise(partitions);
        return stateStore;
    }

    private S3StateStore getStateStore(Schema schema,
                                       List<Partition> partitions) throws StateStoreException {
        return getStateStore(schema, partitions, 0);
    }

    private S3StateStore getStateStoreFromSplitPoints(Schema schema, List<Object> splitPoints) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, splitPoints).construct(), 0);
    }

    private S3StateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStoreFromSplitPoints(schema, Collections.emptyList());
    }
}
