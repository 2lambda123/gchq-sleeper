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
package sleeper.splitter;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.facebook.collections.ByteArray;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.core.CommonTestConstants;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.testutils.IngestCoordinatorTestHelper;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@Testcontainers
public class SplitPartitionIT {
    private static final int DYNAMO_PORT = 8000;
    @Container
    public static GenericContainer<?> dynamoDb = new GenericContainer<>(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);
    private static AmazonDynamoDB dynamoDBClient;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private final Field field = new Field("key", new IntType());
    private final Schema schema = Schema.builder().rowKeyFields(field).build();

    @BeforeAll
    public static void initDynamoClient() {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
    }

    @AfterAll
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    private static StateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    private static StateStore getStateStore(Schema schema, List<Partition> partitions) throws StateStoreException {
        String id = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(id, schema, dynamoDBClient);
        StateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise(partitions);
        return dynamoStateStore;
    }

    @Test
    public void shouldSplitPartitionForIntKeyCorrectly() throws Exception {
        // Given
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 100 * i; r < 100 * (i + 1); r++) {
                Record record = new Record();
                record.put("key", r);
                records.add(record);
            }
            ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        partitionSplitter.splitPartition(rootPartition, stateStore.getActiveFiles().stream().map(FileInfo::getFilename).collect(Collectors.toList()));

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertThat(nonLeafPartitions).hasSize(1);
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Object splitPoint = splitPoint(it.next(), it.next(), "key");
        assertThat(leafPartitions)
                .extracting(partition -> partition.getRegion().getRange("key"))
                .extracting(Range::getMin, Range::getMax)
                .containsExactlyInAnyOrder(
                        tuple(Integer.MIN_VALUE, splitPoint),
                        tuple(splitPoint, null));
        assertThat((int) splitPoint).isStrictlyBetween(400, 600);
        assertThat(leafPartitions).allSatisfy(partition -> {
            assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
            assertThat(partition.getChildPartitionIds()).isEmpty();
        });
        assertThat(nonLeafPartitions).containsExactly(rootPartition);
    }

    @Test
    public void shouldNotSplitPartitionForIntKeyIfItCannotBeSplitBecausePartitionIsOnePoint() throws Exception {
        // Given
        // Non-leaf partitions
        Range rootRange = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null);
        Partition rootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(rootRange))
                .region(new Region(rootRange))
                .id("root")
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .dimension(0)
                .build();
        Range range12 = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 1);
        Partition partition12 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range12))
                .id("id12")
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(0)
                .build();
        // Leaf partitions
        Range range1 = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 0);
        Partition partition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range1))
                .id("id1")
                .leafPartition(true)
                .parentPartitionId(partition12.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        //  - Partition 2 only includes the key 0 (partitions do not include
        //      the maximum key), and so cannot be split.
        Range range2 = new RangeFactory(schema).createRange(field, 0, 1);
        Partition partition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range2))
                .id("id2")
                .leafPartition(true)
                .parentPartitionId(partition12.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Range range3 = new RangeFactory(schema).createRange(field, 1, null);
        Partition partition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range3))
                .id("id3")
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        // Wire up partitions
        rootPartition.setChildPartitionIds(Arrays.asList(partition12.getId(), partition3.getId()));
        partition12.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        //
        List<Partition> partitions = Arrays.asList(rootPartition, partition12, partition1, partition2, partition3);
        StateStore stateStore = getStateStore(schema, partitions);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        for (Partition partition : partitions) {
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                int j = 0;
                int minRange = (int) partition.getRegion().getRange("key").getMin();
                int maxRange = null == partition.getRegion().getRange("key").getMax() ? Integer.MAX_VALUE : (int) partition.getRegion().getRange("key").getMax();
                for (int r = minRange; r < maxRange && j < 10; r++, j++) {
                    Record record = new Record();
                    record.put("key", r);
                    records.add(record);
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(partition2.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(partition2, fileNames);

        // Then
        List<Partition> partitionsAfterSplit = stateStore.getAllPartitions();
        assertThat(partitionsAfterSplit).hasSameSizeAs(partitions);
        assertThat(new HashSet<>(partitionsAfterSplit)).isEqualTo(new HashSet<>(partitions));
    }

    @Test
    public void shouldNotSplitPartitionForIntKeyIfItCannotBeSplitBecauseDataIsConstant() throws Exception {
        // Given
        // Non-leaf partitions
        Range rootRange = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null);
        Partition rootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(rootRange))
                .id("root")
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .dimension(0)
                .build();
        Range range12 = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 10);
        Partition partition12 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range12))
                .id("id12")
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(0)
                .build();
        // Leaf partitions
        Range range1 = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 0);
        Partition partition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range1))
                .id("id1")
                .leafPartition(true)
                .parentPartitionId(partition12.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        //  - Partition 2 only includes the key 0 (partitions do not include
        //      the maximum key), and so cannot be split.
        Range range2 = new RangeFactory(schema).createRange(field, 0, 10);
        Partition partition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range2))
                .id("id2")
                .leafPartition(true)
                .parentPartitionId(partition12.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Range range3 = new RangeFactory(schema).createRange(field, 10, null);
        Partition partition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range3))
                .id("id3")
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        // Wire up partitions
        rootPartition.setChildPartitionIds(Arrays.asList(partition12.getId(), partition3.getId()));
        partition12.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        //
        List<Partition> partitions = Arrays.asList(rootPartition, partition12, partition1, partition2, partition3);
        StateStore stateStore = getStateStore(schema, partitions);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        for (Partition partition : partitions) {
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                int j = 0;
                if (!partition.equals(partition2)) {
                    int minRange = (int) partition.getRegion().getRange("key").getMin();
                    int maxRange = null == partition.getRegion().getRange("key").getMax() ? Integer.MAX_VALUE : (int) partition.getRegion().getRange("key").getMax();
                    for (int r = minRange; r < maxRange && j < 10; r++, j++) {
                        Record record = new Record();
                        record.put("key", r);
                        records.add(record);
                    }
                } else {
                    // Files in partition2 all have the same value for the key
                    for (int r = 0; r < 10; r++) {
                        Record record = new Record();
                        record.put("key", 1);
                        records.add(record);
                    }
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(partition2.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(partition2, fileNames);

        // Then
        List<Partition> partitionsAfterSplit = stateStore.getAllPartitions();
        assertThat(partitionsAfterSplit).hasSameSizeAs(partitions);
        assertThat(new HashSet<>(partitionsAfterSplit)).isEqualTo(new HashSet<>(partitions));
    }

    @Test
    public void shouldSplitPartitionForIntMultidimensionalKeyOnFirstDimensionCorrectly() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                .build();
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key1", r);
                record.put("key2", 10);
                records.add(record);
            }
            ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        //  - There should be 3 partitions
        List<Partition> partitions = stateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);
        //  - There should be 1 non-leaf partition
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        //  - The root partition should have been split on the first dimension
        assertThat(nonLeafPartitions).hasSize(1)
                .extracting(Partition::getDimension)
                .containsExactly(0);
        //  - The leaf partitions should have been split on a value which is between
        //      0 and 100.
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Object splitPoint = splitPoint(it.next(), it.next(), "key1");
        assertThat(leafPartitions)
                .extracting(partition -> partition.getRegion().getRange("key1"))
                .extracting(Range::getMin, Range::getMax)
                .containsExactlyInAnyOrder(
                        tuple(Integer.MIN_VALUE, splitPoint),
                        tuple(splitPoint, null));
        assertThat((int) splitPoint).isStrictlyBetween(Integer.MIN_VALUE, 99);
        //  - The leaf partitions should have the root partition as their parent
        //      and an empty array for the child partitions.
        assertThat(leafPartitions).allSatisfy(partition -> {
            assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
            assertThat(partition.getChildPartitionIds()).isEmpty();
        });
        assertThat(nonLeafPartitions).containsExactly(rootPartition);
    }

    @Test
    public void shouldSplitPartitionForIntMultidimensionalKeyOnSecondDimensionCorrectlyWhenMinIsMax() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                .build();
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key1", 10);
                record.put("key2", r);
                records.add(record);
            }
            ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        //  - There should be 3 partitions
        List<Partition> partitions = stateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);
        //  - There should be 1 non-leaf partition
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        //  - The root partition should have been split on the second dimension
        assertThat(nonLeafPartitions).hasSize(1)
                .extracting(Partition::getDimension)
                .containsExactly(1);
        //  - The leaf partitions should have been split on a value which is between
        //      0 and 100.
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Object splitPoint = splitPoint(it.next(), it.next(), "key2");
        assertThat(leafPartitions)
                .extracting(partition -> partition.getRegion().getRange("key2"))
                .extracting(Range::getMin, Range::getMax)
                .containsExactlyInAnyOrder(
                        tuple(Integer.MIN_VALUE, splitPoint),
                        tuple(splitPoint, null));
        assertThat((int) splitPoint).isStrictlyBetween(Integer.MIN_VALUE, 99);
        //  - The leaf partitions should have the root partition as their parent
        //      and an empty array for the child partitions.
        assertThat(leafPartitions).allSatisfy(partition -> {
            assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
            assertThat(partition.getChildPartitionIds()).isEmpty();
        });
        assertThat(nonLeafPartitions).containsExactly(rootPartition);
    }

    @Test
    public void shouldSplitPartitionForIntMultidimensionalKeyOnSecondDimensionCorrectlyWhenMinIsMedian() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                .build();
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                // The majority of the values are 10; so min should equal median
                if (r < 75) {
                    record.put("key1", 10);
                } else {
                    record.put("key1", 20);
                }
                record.put("key2", r);
                records.add(record);
            }
            ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        //  - There should be 3 partitions
        List<Partition> partitions = stateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);
        //  - There should be 1 non-leaf partition
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        //  - The root partition should have been split on the second dimension
        assertThat(nonLeafPartitions).hasSize(1)
                .extracting(Partition::getDimension)
                .containsExactly(1);
        //  - The leaf partitions should have been split on a value which is between
        //      0 and 100.
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Object splitPoint = splitPoint(it.next(), it.next(), "key2");
        assertThat(leafPartitions)
                .extracting(partition -> partition.getRegion().getRange("key2"))
                .extracting(Range::getMin, Range::getMax)
                .containsExactlyInAnyOrder(
                        tuple(Integer.MIN_VALUE, splitPoint),
                        tuple(splitPoint, null));
        assertThat((int) splitPoint).isStrictlyBetween(Integer.MIN_VALUE, 99);
        //  - The leaf partitions should have the root partition as their parent
        //      and an empty array for the child partitions.
        assertThat(leafPartitions).allSatisfy(partition -> {
            assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
            assertThat(partition.getChildPartitionIds()).isEmpty();
        });
        assertThat(nonLeafPartitions).containsExactly(rootPartition);
    }

    @Test
    public void shouldSplitPartitionForLongKeyCorrectly() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new LongType())).build();
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (long r = 100L * i; r < 100L * (i + 1); r++) {
                Record record = new Record();
                record.put("key", r);
                records.add(record);
            }
            ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertThat(nonLeafPartitions).hasSize(1);
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Object splitPoint = splitPoint(it.next(), it.next(), "key");
        assertThat(leafPartitions)
                .extracting(partition -> partition.getRegion().getRange("key"))
                .extracting(Range::getMin, Range::getMax)
                .containsExactlyInAnyOrder(
                        tuple(Long.MIN_VALUE, splitPoint),
                        tuple(splitPoint, null));
        assertThat((long) splitPoint).isStrictlyBetween(400L, 600L);
        assertThat(leafPartitions).allSatisfy(partition -> {
            assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
            assertThat(partition.getChildPartitionIds()).isEmpty();
        });
        assertThat(nonLeafPartitions).containsExactly(rootPartition);
    }

    @Test
    public void shouldSplitPartitionForStringKeyCorrectly() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key", "A" + i + "" + r);
                records.add(record);
            }
            ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertThat(nonLeafPartitions).hasSize(1);
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        String splitPoint;
        String minRowkey1 = (String) leafPartition1.getRegion().getRange("key").getMin();
        String minRowkey2 = (String) leafPartition2.getRegion().getRange("key").getMin();
        String maxRowKey1 = (String) leafPartition1.getRegion().getRange("key").getMax();
        String maxRowKey2 = (String) leafPartition2.getRegion().getRange("key").getMax();
        if ("".equals(minRowkey1)) {
            splitPoint = maxRowKey1;
            assertThat(minRowkey2).isEqualTo(maxRowKey1);
            assertThat(maxRowKey2).isNull();
        } else {
            splitPoint = maxRowKey2;
            assertThat(minRowkey2).isEmpty();
            assertThat(minRowkey1).isEqualTo(maxRowKey2);
            assertThat(maxRowKey1).isNull();
        }
        assertThat("A00".compareTo(splitPoint) < 0 && splitPoint.compareTo("A9100") < 0).isTrue();
        assertThat(leafPartitions).allSatisfy(partition -> {
            assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
            assertThat(partition.getChildPartitionIds()).isEmpty();
        });
        assertThat(nonLeafPartitions).containsExactly(rootPartition);
    }

    @Test
    public void shouldSplitPartitionForByteArrayKeyCorrectly()
            throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new ByteArrayType())).build();
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key", new byte[]{(byte) r});
                records.add(record);
            }
            ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertThat(nonLeafPartitions).hasSize(1);
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        byte[] splitPoint = splitPointBytes(it.next(), it.next(), "key");
        assertThat(leafPartitions)
                .extracting(partition -> partition.getRegion().getRange("key"))
                .extracting(Range::getMin, Range::getMax)
                .containsExactlyInAnyOrder(
                        tuple(new byte[]{}, splitPoint),
                        tuple(splitPoint, null));
        assertThat(ByteArray.wrap(splitPoint)).isStrictlyBetween(
                ByteArray.wrap(new byte[]{}),
                ByteArray.wrap(new byte[]{99}));
        assertThat(leafPartitions).allSatisfy(partition -> {
            assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
            assertThat(partition.getChildPartitionIds()).isEmpty();
        });
        assertThat(nonLeafPartitions).containsExactly(rootPartition);
    }

    @Test
    public void shouldNotSplitPartitionForByteArrayKeyIfItCannotBeSplitBecausePartitionIsOnePoint() throws Exception {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        // Non-leaf partitions
        Range rootRange = new RangeFactory(schema).createRange(field, new byte[]{0}, null);
        Partition rootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(rootRange))
                .id("root")
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .dimension(0)
                .build();
        Range range12 = new RangeFactory(schema).createRange(field, new byte[]{0}, new byte[]{51});
        Partition partition12 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range12))
                .id("id12")
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(0)
                .build();
        // Leaf partitions
        Range range1 = new RangeFactory(schema).createRange(field, new byte[]{0}, new byte[]{50});
        Partition partition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range1))
                .id("id1")
                .leafPartition(true)
                .parentPartitionId(partition12.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        //  - Partition 2 only includes the key 0 (partitions do not include
        //      the maximum key), and so cannot be split.
        Range range2 = new RangeFactory(schema).createRange(field, new byte[]{50}, new byte[]{51});
        Partition partition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range2))
                .id("id2")
                .leafPartition(true)
                .parentPartitionId(partition12.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Range range3 = new RangeFactory(schema).createRange(field, new byte[]{51}, null);
        Partition partition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range3))
                .id("id3")
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        // Wire up partitions
        rootPartition.setChildPartitionIds(Arrays.asList(partition12.getId(), partition3.getId()));
        partition12.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        //
        List<Partition> partitions = Arrays.asList(rootPartition, partition12, partition1, partition2, partition3);
        StateStore stateStore = getStateStore(schema, partitions);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        for (Partition partition : partitions) {
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (int r = 0; r < 100; r++) {
                    Record record = new Record();
                    record.put("key", new byte[]{(byte) r});
                    records.add(record);
                }
                if (partition.equals(partition1)) {
                    int j = 0;
                    for (byte r = ((byte[]) partition.getRegion().getRange("key").getMin())[0];
                         r < ((byte[]) partition.getRegion().getRange("key").getMax())[0] && j < 10;
                         r++, j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{r});
                        records.add(record);
                    }
                } else if (partition.equals(partition2)) {
                    for (int j = 0; j < 10; j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{50});
                        records.add(record);
                    }
                } else {
                    for (int j = 51; j < 60; j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{(byte) j});
                        records.add(record);
                    }
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(partition2.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(partition2, fileNames);

        // Then
        List<Partition> partitionsAfterSplit = stateStore.getAllPartitions();
        assertThat(partitionsAfterSplit).hasSameSizeAs(partitions);
        assertThat(new HashSet<>(partitionsAfterSplit)).isEqualTo(new HashSet<>(partitions));
    }

    @Test
    public void shouldNotSplitPartitionForByteArrayKeyIfItCannotBeSplitBecauseDataIsConstant() throws Exception {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        // Non-leaf partitions
        Range rootRange = new RangeFactory(schema).createRange(field, new byte[]{0}, null);
        Partition rootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(rootRange))
                .id("root")
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .dimension(0)
                .build();
        Range range12 = new RangeFactory(schema).createRange(field, new byte[]{0}, new byte[]{100});
        Partition partition12 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range12))
                .id("id12")
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(0)
                .build();
        // Leaf partitions
        Range range1 = new RangeFactory(schema).createRange(field, new byte[]{0}, new byte[]{50});
        Partition partition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range1))
                .id("id1")
                .leafPartition(true)
                .parentPartitionId(partition12.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        //  - Partition 2 only includes the key 0 (partitions do not include
        //      the maximum key), and so cannot be split.
        Range range2 = new RangeFactory(schema).createRange(field, new byte[]{50}, new byte[]{100});
        Partition partition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range2))
                .id("id2")
                .leafPartition(true)
                .parentPartitionId(partition12.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Range range3 = new RangeFactory(schema).createRange(field, new byte[]{100}, null);
        Partition partition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range3))
                .id("id3")
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        // Wire up partitions
        rootPartition.setChildPartitionIds(Arrays.asList(partition12.getId(), partition3.getId()));
        partition12.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        //
        List<Partition> partitions = Arrays.asList(rootPartition, partition12, partition1, partition2, partition3);
        StateStore stateStore = getStateStore(schema, partitions);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        for (Partition partition : partitions) {
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                if (partition.equals(partition1)) {
                    int j = 0;
                    for (byte r = ((byte[]) partition.getRegion().getRange("key").getMin())[0];
                         r < ((byte[]) partition.getRegion().getRange("key").getMax())[0] && j < 10;
                         r++, j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{r});
                        records.add(record);
                    }
                } else if (partition.equals(partition2)) {
                    // Files in partition2 all have the same value for the key
                    for (int j = 0; j < 10; j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{60});
                        records.add(record);
                    }
                } else {
                    for (int j = 100; j < 110; j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{(byte) j});
                        records.add(record);
                    }
                }
                ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
            }
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(partition2.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(partition2, fileNames);

        // Then
        List<Partition> partitionsAfterSplit = stateStore.getAllPartitions();
        assertThat(partitionsAfterSplit).hasSameSizeAs(partitions);
        assertThat(new HashSet<>(partitionsAfterSplit)).isEqualTo(new HashSet<>(partitions));
    }

    @Test
    public void shouldSplitPartitionForByteArrayMultidimensionalKeyOnFirstDimensionCorrectly() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()))
                .build();
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key1", new byte[]{(byte) r});
                record.put("key2", new byte[]{(byte) -100});
                records.add(record);
            }
            ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        //  - There should be 3 partitions
        List<Partition> partitions = stateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);
        //  - There should be 1 non-leaf partition
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        //  - The root partition should have been split on the first dimension
        assertThat(nonLeafPartitions).hasSize(1)
                .extracting(Partition::getDimension)
                .containsExactly(0);
        //  - The leaf partitions should have been split on a value which is between
        //      0 and 100.
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        byte[] splitPoint = splitPointBytes(it.next(), it.next(), "key1");
        assertThat(leafPartitions)
                .extracting(partition -> partition.getRegion().getRange("key1"))
                .extracting(Range::getMin, Range::getMax)
                .containsExactlyInAnyOrder(
                        tuple(new byte[]{}, splitPoint),
                        tuple(splitPoint, null));
        assertThat(ByteArray.wrap(splitPoint)).isStrictlyBetween(
                ByteArray.wrap(new byte[]{}),
                ByteArray.wrap(new byte[]{99}));
        //  - The leaf partitions should have the root partition as their parent
        //      and an empty array for the child partitions.
        assertThat(leafPartitions).allSatisfy(partition -> {
            assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
            assertThat(partition.getChildPartitionIds()).isEmpty();
        });
        assertThat(nonLeafPartitions).containsExactly(rootPartition);
    }

    @Test
    public void shouldSplitPartitionForByteArrayMultidimensionalKeyOnSecondDimensionCorrectly() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()))
                .build();
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key1", new byte[]{(byte) -100});
                record.put("key2", new byte[]{(byte) r});
                records.add(record);
            }
            ingestRecordsFromIterator(stateStore, schema, path, path2, records.iterator());
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertThat(nonLeafPartitions).hasSize(1)
                .extracting(Partition::getDimension)
                .containsExactly(1);
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        byte[] splitPoint = splitPointBytes(it.next(), it.next(), "key2");
        assertThat(leafPartitions)
                .extracting(partition -> partition.getRegion().getRange("key2"))
                .extracting(Range::getMin, Range::getMax)
                .containsExactlyInAnyOrder(
                        tuple(new byte[]{}, splitPoint),
                        tuple(splitPoint, null));
        assertThat(ByteArray.wrap(splitPoint)).isStrictlyBetween(
                ByteArray.wrap(new byte[]{}),
                ByteArray.wrap(new byte[]{99}));
        assertThat(leafPartitions).allSatisfy(partition -> {
            assertThat(partition.getParentPartitionId()).isEqualTo(rootPartition.getId());
            assertThat(partition.getChildPartitionIds()).isEmpty();
        });
        assertThat(nonLeafPartitions).containsExactly(rootPartition);
    }

    private static byte[] splitPointBytes(Partition partition1, Partition partition2, String key) {
        Range range1 = partition1.getRegion().getRange(key);
        Range range2 = partition2.getRegion().getRange(key);
        if (Arrays.equals((byte[]) range1.getMin(), (byte[]) range2.getMax())) {
            return (byte[]) range1.getMin();
        } else {
            return (byte[]) range1.getMax();
        }
    }

    private static Object splitPoint(Partition partition1, Partition partition2, String key) {
        Range range1 = partition1.getRegion().getRange(key);
        Range range2 = partition2.getRegion().getRange(key);
        if (Objects.equals(range1.getMin(), range2.getMax())) {
            return range1.getMin();
        } else {
            return range1.getMax();
        }
    }

    private static void ingestRecordsFromIterator(StateStore stateStore, Schema schema, String localDir,
                                                  String filePathPrefix, Iterator<Record> recordIterator) throws Exception {
        ParquetConfiguration parquetConfiguration = IngestCoordinatorTestHelper.parquetConfiguration(schema, new Configuration());
        IngestCoordinator<Record> ingestCoordinator = IngestCoordinatorTestHelper.standardIngestCoordinator(
                stateStore, schema,
                ArrayListRecordBatchFactory.builder()
                        .localWorkingDirectory(localDir)
                        .maxNoOfRecordsInMemory(1000000)
                        .maxNoOfRecordsInLocalStore(100000000)
                        .parquetConfiguration(parquetConfiguration)
                        .buildAcceptingRecords(),
                DirectPartitionFileWriterFactory.from(parquetConfiguration, filePathPrefix));
        new IngestRecordsFromIterator(ingestCoordinator, recordIterator).write();
    }
}
