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
package sleeper.core.partition;

import com.facebook.collections.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Given a list of split points that split the first dimension of the row keys into partitions, this class
 * constructs a full tree of partitions, up to a single root.
 */
public class PartitionsFromSplitPoints {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionsFromSplitPoints.class);

    private final Schema schema;
    private final List<Field> rowKeyFields;
    private final List<PrimitiveType> rowKeyTypes;
    private final List<Object> splitPoints;
    private final RangeFactory rangeFactory;

    public PartitionsFromSplitPoints(
            Schema schema, List<Object> splitPoints) {
        this.schema = schema;
        this.rowKeyFields = schema.getRowKeyFields();
        this.rowKeyTypes = new ArrayList<>();
        for (Field field : rowKeyFields) {
            this.rowKeyTypes.add((PrimitiveType) field.getType());
        }
        this.splitPoints = splitPoints;
        this.rangeFactory = new RangeFactory(schema);
    }

    public List<Partition> construct() {
        // If there are no split points then create a single root partition, which covers the entire key space, and
        // is a leaf partition.
        if (null == splitPoints || splitPoints.isEmpty()) {
            LOGGER.info("Constructing partition tree from no split points - tree will consist of one partition");
            return Collections.singletonList(createRootPartitionThatIsLeaf());
        }

        validateSplitPoints();
        LOGGER.info("Split points are valid");

        // There is at least 1 split point. Use the split points to create leaf partitions.
        List<Partition> leafPartitions = createLeafPartitions();
        List<Partition> allPartitions = new ArrayList<>(leafPartitions);

        List<Partition> nextLayer = addLayer(leafPartitions, allPartitions);
        while (1 != nextLayer.size()) {
            nextLayer = addLayer(nextLayer, allPartitions);
        }

        return allPartitions;
    }

    private List<Partition> addLayer(List<Partition> partitionsInLayer, List<Partition> allPartitions) {
        List<Partition> parents = new ArrayList<>();
        for (int i = 0; i < partitionsInLayer.size(); i += 2) {
            if (i <= partitionsInLayer.size() - 2) {
                Partition leftPartition = partitionsInLayer.get(i);
                Partition rightPartition = partitionsInLayer.get(i + 1);

                Partition parent = new Partition();
                parent.setId(UUID.randomUUID().toString());
                parent.setParentPartitionId(null);
                parent.setChildPartitionIds(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
                parent.setLeafPartition(false);
                parent.setDimension(0);
                parent.setRowKeyTypes(leftPartition.getRowKeyTypes());
                List<Range> ranges = new ArrayList<>();
                for (Range range : leftPartition.getRegion().getRanges()) {
                    if (!range.getFieldName().equals(rowKeyFields.get(0).getName())) {
                        ranges.add(range); // TODO Check that left and right have the same ranges in the dimensions other than 0
                    }
                }
                Range rangeForDim0 = rangeFactory.createRange(rowKeyFields.get(0),
                        leftPartition.getRegion().getRange(rowKeyFields.get(0).getName()).getMin(),
                        true,
                        rightPartition.getRegion().getRange(rowKeyFields.get(0).getName()).getMax(),
                        false);
                ranges.add(rangeForDim0);
                Region region = new Region(ranges);
                parent.setRegion(region);

                leftPartition.setParentPartitionId(parent.getId());
                rightPartition.setParentPartitionId(parent.getId());

                parents.add(parent);
            }
        }
        allPartitions.addAll(parents);

        // If there were an odd number of partitions in partitionsInLayer then we need to add the remaining one into
        // the next layer, but it shouldn't be added into allPartitions as it is already there.
        if (partitionsInLayer.size() % 2 == 1) {
            parents.add(partitionsInLayer.get(partitionsInLayer.size() - 1));
        }

        LOGGER.info("Created layer of {} partitions from previous layer of {} partitions", parents.size(), partitionsInLayer.size());
        LOGGER.debug("New partitions are {}", parents);

        return parents;
    }

    private List<Partition> createLeafPartitions() {
        List<Region> leafRegions = leafRegionsFromSplitPoints(schema, splitPoints);
        List<Partition> leafPartitions = new ArrayList<>();
        for (Region region : leafRegions) {
            Partition partition = new Partition();
            partition.setRowKeyTypes(rowKeyTypes);
            partition.setRegion(region);
            partition.setId(UUID.randomUUID().toString());
            partition.setLeafPartition(true);
            partition.setParentPartitionId(null);
            partition.setChildPartitionIds(new ArrayList<>());
            partition.setDimension(-1);
            leafPartitions.add(partition);
        }
        LOGGER.info("Created {} leaf partitions from {} split points", leafPartitions.size(), splitPoints.size());
        LOGGER.debug("Partitions are {}", leafPartitions);
        return leafPartitions;
    }

    private Partition createRootPartitionThatIsLeaf() {
        Partition rootPartition = new Partition();
        rootPartition.setRowKeyTypes(rowKeyTypes);
        List<Range> ranges = new ArrayList<>();
        for (Field field : rowKeyFields) {
            ranges.add(getRangeCoveringWholeDimension(field));
        }
        Region region = new Region(ranges);
        rootPartition.setRegion(region);
        rootPartition.setId("root");
        rootPartition.setLeafPartition(true);
        rootPartition.setParentPartitionId(null);
        rootPartition.setChildPartitionIds(new ArrayList<>());
        rootPartition.setDimension(-1);
        return rootPartition;
    }

    private Range getRangeCoveringWholeDimension(Field field) {
        Range range = rangeFactory.createRange(field, getMinimum(field.getType()), true, null, false);
        return range;
    }

    private static Object getMinimum(Type type) {
        if (type instanceof IntType) {
            return Integer.MIN_VALUE;
        }
        if (type instanceof LongType) {
            return Long.MIN_VALUE;
        }
        if (type instanceof StringType) {
            return "";
        }
        if (type instanceof ByteArrayType) {
            return new byte[]{};
        }
        throw new IllegalArgumentException("Unknown key type " + type);
    }

    private void validateSplitPoints() {
        int count = 0;
        Comparable previous = null;
        for (Object obj : splitPoints) {
            validateCorrectType(obj);
            Comparable comparable = getAsComparable(obj);
            if (count > 0) {
                if (previous.compareTo(comparable) >= 0) {
                    throw new IllegalArgumentException("Invalid split point: " + previous + " should be less than " + comparable);
                }
            }
            previous = comparable;
            count++;
        }
    }

    private void validateCorrectType(Object obj) {
        Type type = rowKeyTypes.get(0);
        if (type instanceof IntType) {
            if (!(obj instanceof Integer)) {
                throw new IllegalArgumentException("Invalid split point: " + obj + " should be of type Integer");
            }
        } else if (type instanceof LongType) {
            if (!(obj instanceof Long)) {
                throw new IllegalArgumentException("Invalid split point: " + obj + " should be of type Long");
            }
        } else if (type instanceof StringType) {
            if (!(obj instanceof String)) {
                throw new IllegalArgumentException("Invalid split point: " + obj + " should be of type String");
            }
        } else if (type instanceof ByteArrayType) {
            if (!(obj instanceof byte[])) {
                throw new IllegalArgumentException("Invalid split point: " + obj + " should be of type byte[]");
            }
        } else {
            throw new IllegalArgumentException("Unknown key type " + type);
        }
    }

    private Comparable getAsComparable(Object obj) {
        Type type = rowKeyTypes.get(0);
        if (type instanceof ByteArrayType) {
            return ByteArray.wrap((byte[]) obj);
        }
        return (Comparable) obj;
    }

    public static List<Region> leafRegionsFromSplitPoints(Schema schema, List<Object> splitPoints) {
        RangeFactory rangeFactoryStatic = new RangeFactory(schema);
        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        List<Field> rowKeyFields = schema.getRowKeyFields();
        List<Object> partitionBoundaries = new ArrayList<>();
        Type type = rowKeyTypes.get(0);
        partitionBoundaries.add(getMinimum(type));
        partitionBoundaries.addAll(splitPoints);
        partitionBoundaries.add(null);

        // Create ranges for the other dimensions
        List<Range> ranges = new ArrayList<>();
        for (Field rowKeyField : rowKeyFields.subList(1, rowKeyFields.size())) {
            Type rowKeyType = rowKeyField.getType();
            Range range = rangeFactoryStatic.createRange(rowKeyField, getMinimum(rowKeyType), true, null, false);
            ranges.add(range);
        }

        List<Region> leafRegions = new ArrayList<>();
        for (int i = 0; i < partitionBoundaries.size() - 1; i++) {
            List<Range> rangesForThisRegion = new ArrayList<>();
            Range rangeForDim0 = rangeFactoryStatic.createRange(rowKeyFields.get(0), partitionBoundaries.get(i), true, partitionBoundaries.get(i + 1), false);
            rangesForThisRegion.add(rangeForDim0);
            rangesForThisRegion.addAll(ranges);
            Region region = new Region(rangesForThisRegion);
            leafRegions.add(region);
        }
        return leafRegions;
    }

    public static PartitionTree treeFrom(Schema schema, List<Object> splitPoints) {
        return new PartitionTree(schema, new PartitionsFromSplitPoints(schema, splitPoints).construct());
    }

}
