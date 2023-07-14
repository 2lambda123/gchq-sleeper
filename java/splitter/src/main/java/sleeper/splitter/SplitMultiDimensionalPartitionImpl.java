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

import com.facebook.collections.ByteArray;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
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
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Identifies the median value of the first dimension. If that leads to a valid
 * split (i.e. one where it is not equal to the minimum value and not equal to
 * the maximum value) then that is used to split the partition. If it doesn't
 * lead to a valid split then the above is repeated for the second dimension.
 * This continues until either a valid split is found or no split is possible.
 * <p>
 * Note that there are two situations in which a partition cannot be split:
 * - If the partition consists of a single point (i.e. the minimum
 * equals the maximum).
 * - If the median equals the minimum then the partition cannot be split.
 * This is because it would have to be split into [min, median) and [median, max),
 * but if the min equals the median then the left one can't have any data in it
 * as a key x in it would have to have min <= x < median = min which is a
 * contradiction.
 * <p>
 */
public class SplitMultiDimensionalPartitionImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitMultiDimensionalPartitionImpl.class);

    private final StateStore stateStore;
    private final Schema schema;
    private final List<PrimitiveType> rowKeyTypes;
    private final Partition partition;
    private final List<String> fileNames; // These should be active files for the partition
    private final Configuration conf;
    private final RangeFactory rangeFactory;
    private final Supplier<String> idSupplier;

    public SplitMultiDimensionalPartitionImpl(StateStore stateStore,
                                              Schema schema,
                                              Partition partition,
                                              List<String> fileNames,
                                              Configuration conf,
                                              Supplier<String> idSupplier) {
        this.stateStore = stateStore;
        this.schema = schema;
        this.rowKeyTypes = schema.getRowKeyTypes();
        this.partition = partition;
        this.fileNames = fileNames;
        this.conf = conf;
        this.rangeFactory = new RangeFactory(schema);
        this.idSupplier = idSupplier;
    }

    void splitPartition() throws StateStoreException, IOException {
        for (int dimension = 0; dimension < rowKeyTypes.size(); dimension++) {
            PrimitiveType rowKeyType = rowKeyTypes.get(dimension);
            LOGGER.info("Testing field {} of type {} (dimension {}) to see if it can be split",
                    schema.getRowKeyFieldNames().get(dimension), rowKeyType, dimension);
            if (rowKeyType instanceof IntType) {
                Triple<Integer, Integer, Integer> minMedianMax = getMinMedianMaxIntKey(dimension);
                int min = minMedianMax.getLeft();
                int median = minMedianMax.getMiddle();
                int max = minMedianMax.getRight();
                LOGGER.debug("Min = {}, median = {}, max = {}", min, median, max);
                if (min > max) {
                    throw new RuntimeException("Min > max");
                }
                if (min < median && median < max) {
                    LOGGER.info("For dimension {} min < median && median < max, so splitting", dimension);
                    splitPartition(partition, median, dimension);
                    return;
                } else {
                    LOGGER.info("For dimension {} it is not true that min < median && median < max, so NOT splitting", dimension);
                }
            } else if (rowKeyType instanceof LongType) {
                Triple<Long, Long, Long> minMedianMax = getMinMedianMaxLongKey(dimension);
                long min = minMedianMax.getLeft();
                long median = minMedianMax.getMiddle();
                long max = minMedianMax.getRight();
                LOGGER.debug("Min = {}, median = {}, max = {}", min, median, max);
                if (min > max) {
                    throw new RuntimeException("Min > max");
                }
                if (min < median && median < max) {
                    LOGGER.debug("For dimension {} min < median && median < max", dimension);
                    splitPartition(partition, median, dimension);
                    return;
                } else {
                    LOGGER.info("For dimension {} it is not true that min < median && median < max, so NOT splitting", dimension);
                }
            } else if (rowKeyType instanceof StringType) {
                Triple<String, String, String> minMedianMax = getMinMedianMaxStringKey(dimension);
                String min = minMedianMax.getLeft();
                String median = minMedianMax.getMiddle();
                String max = minMedianMax.getRight();
                LOGGER.debug("Min = {}, median = {}, max = {}", min, median, max);
                if (min.compareTo(max) > 0) {
                    throw new RuntimeException("Min > max");
                }
                if (min.compareTo(median) < 0 && median.compareTo(max) < 0) {
                    LOGGER.debug("For dimension {} min < median && median < max", dimension);
                    splitPartition(partition, median, dimension);
                    return;
                } else {
                    LOGGER.info("For dimension {} it is not true that min < median && median < max, so NOT splitting", dimension);
                }
            } else if (rowKeyType instanceof ByteArrayType) {
                Triple<ByteArray, ByteArray, ByteArray> minMedianMax = getMinMedianMaxByteArrayKey(dimension);
                ByteArray min = minMedianMax.getLeft();
                ByteArray median = minMedianMax.getMiddle();
                ByteArray max = minMedianMax.getRight();
                LOGGER.debug("Min = {}, median = {}, max = {}", min, median, max);
                if (min.compareTo(max) > 0) {
                    throw new RuntimeException("Min > max");
                }
                if (min.compareTo(median) < 0 && median.compareTo(max) < 0) {
                    LOGGER.debug("For dimension {} min < median && median < max", dimension);
                    splitPartition(partition, median.getArray(), dimension);
                    return;
                } else {
                    LOGGER.info("For dimension {} it is not true that min < median && median < max, so NOT splitting", dimension);
                }
            } else {
                throw new RuntimeException("Unknown type " + rowKeyType);
            }
        }
    }

    private Triple<Integer, Integer, Integer> getMinMedianMaxIntKey(int dimension) throws IOException {
        String keyField = schema.getRowKeyFields().get(dimension).getName();

        // Read all sketches
        List<ItemsSketch<Integer>> sketchList = new ArrayList<>();
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = new SketchesSerDeToS3(schema).loadFromHadoopFS(new Path(sketchesFile), conf);
            sketchList.add((ItemsSketch<Integer>) sketches.getQuantilesSketch(keyField));
        }

        // Union all the sketches
        ItemsUnion<Integer> union = ItemsUnion.getInstance(16384, Comparator.naturalOrder());
        for (ItemsSketch<Integer> s : sketchList) {
            union.update(s);
        }
        ItemsSketch<Integer> sketch = union.getResult();

        Integer min = sketch.getMinValue();
        Integer median = sketch.getQuantile(0.5D);
        Integer max = sketch.getMaxValue();
        return new ImmutableTriple<>(min, median, max);
    }

    private Triple<Long, Long, Long> getMinMedianMaxLongKey(int dimension) throws IOException {
        String keyField = schema.getRowKeyFields().get(dimension).getName();

        // Read all sketches
        List<ItemsSketch<Long>> sketchList = new ArrayList<>();
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = new SketchesSerDeToS3(schema).loadFromHadoopFS(new Path(sketchesFile), conf);
            sketchList.add((ItemsSketch<Long>) sketches.getQuantilesSketch(keyField));
        }

        // Union all the sketches
        ItemsUnion<Long> union = ItemsUnion.getInstance(16384, Comparator.naturalOrder());
        for (ItemsSketch<Long> s : sketchList) {
            union.update(s);
        }
        ItemsSketch<Long> sketch = union.getResult();

        Long min = sketch.getMinValue();
        Long median = sketch.getQuantile(0.5D);
        Long max = sketch.getMaxValue();
        return new ImmutableTriple<>(min, median, max);
    }

    private Triple<String, String, String> getMinMedianMaxStringKey(int dimension) throws IOException {
        String keyField = schema.getRowKeyFields().get(dimension).getName();

        // Read all sketches
        List<ItemsSketch<String>> sketchList = new ArrayList<>();
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = new SketchesSerDeToS3(schema).loadFromHadoopFS(new Path(sketchesFile), conf);
            sketchList.add((ItemsSketch<String>) sketches.getQuantilesSketch(keyField));
        }

        // Union all the sketches
        ItemsUnion<String> union = ItemsUnion.getInstance(16384, Comparator.naturalOrder());
        for (ItemsSketch<String> s : sketchList) {
            union.update(s);
        }
        ItemsSketch<String> sketch = union.getResult();

        String min = sketch.getMinValue();
        String median = sketch.getQuantile(0.5D);
        String max = sketch.getMaxValue();
        return new ImmutableTriple<>(min, median, max);
    }

    private Triple<ByteArray, ByteArray, ByteArray> getMinMedianMaxByteArrayKey(int dimension) throws IOException {
        String keyField = schema.getRowKeyFields().get(dimension).getName();

        // Read all sketches
        List<ItemsSketch<ByteArray>> sketchList = new ArrayList<>();
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = new SketchesSerDeToS3(schema).loadFromHadoopFS(new Path(sketchesFile), conf);
            sketchList.add((ItemsSketch<ByteArray>) sketches.getQuantilesSketch(keyField));
        }

        // Union all the sketches
        ItemsUnion<ByteArray> union = ItemsUnion.getInstance(16384, Comparator.naturalOrder());
        for (ItemsSketch<ByteArray> s : sketchList) {
            union.update(s);
        }
        ItemsSketch<ByteArray> sketch = union.getResult();

        ByteArray min = sketch.getMinValue();
        ByteArray median = sketch.getQuantile(0.5D);
        ByteArray max = sketch.getMaxValue();
        return new ImmutableTriple<>(min, median, max);
    }

    private List<Range> removeRange(List<Range> inputRanges, String rangeToRemove) {
        return inputRanges.stream()
                .filter(r -> !r.getFieldName().equals(rangeToRemove))
                .collect(Collectors.toList());
    }

    private void splitPartition(Partition partition, Object splitPoint, int dimension)
            throws StateStoreException {
        Field fieldToSplitOn = schema.getRowKeyFields().get(dimension);
        LOGGER.info("Splitting partition {} on split point {} in dimension {}", partition.getId(), splitPoint, dimension);

        // New partitions
        List<Range> leftChildRanges = removeRange(partition.getRegion().getRanges(), fieldToSplitOn.getName());
        Range rangeForSplitDimensionLeftChild = rangeFactory.createRange(fieldToSplitOn, partition.getRegion().getRange(fieldToSplitOn.getName()).getMin(), splitPoint);
        leftChildRanges.add(rangeForSplitDimensionLeftChild);
        Region leftChildRegion = new Region(leftChildRanges);
        Partition leftChild = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(leftChildRegion)
                .id(idSupplier.get())
                .leafPartition(true)
                .parentPartitionId(partition.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();

        List<Range> rightChildRanges = removeRange(partition.getRegion().getRanges(), fieldToSplitOn.getName());
        Range rangeForSplitDimensionRightChild = rangeFactory.createRange(fieldToSplitOn, splitPoint, partition.getRegion().getRange(fieldToSplitOn.getName()).getMax());
        rightChildRanges.add(rangeForSplitDimensionRightChild);
        Region rightChildRegion = new Region(rightChildRanges);
        Partition rightChild = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(rightChildRegion)
                .id(idSupplier.get())
                .leafPartition(true)
                .parentPartitionId(partition.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();

        // Updated split partition
        partition.setLeafPartition(false);
        partition.setChildPartitionIds(Arrays.asList(leftChild.getId(), rightChild.getId()));
        partition.setDimension(dimension);

        LOGGER.info("Updating StateStore:");
        LOGGER.info("Split partition ({}) is marked as not a leaf partition, split on field {}",
                partition.getId(), fieldToSplitOn.getName());
        LOGGER.info("New partition: {}", leftChild);
        LOGGER.info("New partition: {}", rightChild);

        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(partition, leftChild, rightChild);
    }
}
