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
package sleeper.core.partition;

import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A convenience class for specifying partitions. Used to build {@link Partition} objects.
 * <p>
 * Note that a shorthand is used for cases where we have a schema with only one row key field.
 * This will not be useful in the general case.
 */
public class PartitionFactory {

    private final Schema schema;
    private final Range.RangeFactory rangeFactory;

    public PartitionFactory(Schema schema) {
        this.schema = schema;
        this.rangeFactory = new Range.RangeFactory(schema);
    }

    /**
     * Start a partition covering a range over a single row key field. May only be used for a schema with one row key.
     * Will default to a leaf partition, where the parent partition ID must be set on the builder.
     *
     * @param  id  unique identifier for the new partition
     * @param  min minimum value for the range, inclusive
     * @param  max maximum value for the range, exclusive
     * @return     a builder for the new partition
     */
    public Partition.Builder partition(String id, Object min, Object max) {
        return partition(id, new Region(rangeFactory.createRange(singleRowKeyField(), min, max)));
    }

    /**
     * Start a partition covering a region. Will default to a leaf partition, where the parent partition ID must be set
     * on the builder.
     *
     * @param  id     unique identifier for the new partition
     * @param  region the region the new partition will cover
     * @return        a builder for the new partition
     */
    public Partition.Builder partition(String id, Region region) {
        return Partition.builder()
                .region(region)
                .id(id)
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1);
    }

    /**
     * Split a partition into two new child partitions. The left child will cover values lower than the split point,
     * and the right child will cover values equal to or higher than the split point.
     *
     * @param  parent     partition to split
     * @param  leftId     unique identifier for the partition on the lower side of the split
     * @param  rightId    unique identifier for the partition on the higher side of the split
     * @param  dimension  index in the schema of the row key to split on
     * @param  splitPoint value to split on
     * @return            result of the split, including the new state of the parent and the new child partitions
     */
    public PartitionSplitResult split(Partition parent, String leftId, String rightId, int dimension, Object splitPoint) {
        Field splitField = schema.getRowKeyFields().get(dimension);
        Region parentRegion = parent.getRegion();
        Range parentRange = parentRegion.getRange(splitField.getName());
        Range leftRange = rangeFactory.createRange(splitField, parentRange.getMin(), splitPoint);
        Range rightRange = rangeFactory.createRange(splitField, splitPoint, parentRange.getMax());
        Partition.Builder leftPartition = partition(leftId, parentRegion.copyWithRange(leftRange));
        Partition.Builder rightPartition = partition(rightId, parentRegion.copyWithRange(rightRange));
        leftPartition.parentPartitionId(parent.getId());
        rightPartition.parentPartitionId(parent.getId());
        Partition.Builder updatedParent = parent.toBuilder()
                .leafPartition(false)
                .dimension(dimension)
                .childPartitionIds(List.of(leftId, rightId));
        return PartitionSplitResult.builder()
                .parent(updatedParent)
                .children(List.of(leftPartition, rightPartition))
                .build();
    }

    /**
     * Join two partitions to produce a new parent partition.
     *
     * @param  parentId unique identifier for the new partition
     * @param  left     builder for the left partition, covering values lower than the split point
     * @param  right    builder for the right partition, covering values higher than the split point
     * @return          builder for the new parent partition
     */
    public Partition.Builder parentJoining(String parentId, Partition.Builder left, Partition.Builder right) {
        return parent(Arrays.asList(left, right), parentId, parentRegion(left.getRegion(), right.getRegion()));
    }

    /**
     * Create a new root partition, covering all values for all row keys. This will initially be a leaf partition, but
     * it can be split into child partitions.
     *
     * @param  id unique identifier for the new partition
     * @return    builder for the new root partition
     */
    public Partition.Builder rootFirst(String id) {
        return PartitionsFromSplitPoints
                .createRootPartitionThatIsLeaf(schema, rangeFactory)
                .id(id);
    }

    private Partition.Builder parent(List<Partition.Builder> children, String id, Region region) {
        Partition.Builder parent = partition(id, region)
                .childPartitionIds(children.stream()
                        .map(Partition.Builder::getId)
                        .collect(Collectors.toList()))
                .leafPartition(false)
                .dimension(0);
        children.forEach(child -> child.parentPartitionId(id));
        return parent;
    }

    private Region parentRegion(Region left, Region right) {
        return new Region(schema.getRowKeyFields().stream()
                .map(field -> rangeFactory.createRange(field,
                        left.getRange(field.getName()).getMin(),
                        right.getRange(field.getName()).getMax()))
                .collect(Collectors.toList()));
    }

    private Field singleRowKeyField() {
        List<Field> rowKeyFields = schema.getRowKeyFields();
        if (rowKeyFields.size() != 1) {
            throw new IllegalStateException("Cannot get single row key field, have " + rowKeyFields.size() + " row key fields");
        }
        return rowKeyFields.get(0);
    }
}
