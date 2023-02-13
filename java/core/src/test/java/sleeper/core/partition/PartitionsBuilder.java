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
package sleeper.core.partition;

import sleeper.core.range.Region;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * A convenience class for specifying partitions.
 * <p>
 * Note that a shorthand is used for cases where we have a schema with only one row key field.
 * This will not be useful in the general case.
 */
public class PartitionsBuilder {

    private final Schema schema;
    private final PartitionFactory factory;
    private final List<Partition> partitions = new ArrayList<>();
    private final Map<String, Partition> partitionById = new HashMap<>();

    public PartitionsBuilder(Schema schema) {
        this.schema = schema;
        factory = new PartitionFactory(schema);
    }

    public PartitionsBuilder singlePartition(String id) {
        return leavesWithSplits(Collections.singletonList(id), Collections.emptyList());
    }

    public PartitionsBuilder leavesWithSplits(List<String> ids, List<Object> splits) {
        return leavesWithSplitsOnDimension(0, ids, splits);
    }

    public PartitionsBuilder leavesWithSplitsOnDimension(int dimension, List<String> ids, List<Object> splits) {
        List<Region> regions = PartitionsFromSplitPoints.leafRegionsFromDimensionSplitPoints(schema, dimension, splits);
        if (ids.size() != regions.size()) {
            throw new IllegalArgumentException("Must specify IDs for all leaves before, after and in between splits");
        }
        for (int i = 0; i < ids.size(); i++) {
            add(factory.partition(ids.get(i), regions.get(i)));
        }
        return this;
    }

    public PartitionsBuilder anyTreeJoiningAllLeaves() {
        if (partitions.stream().anyMatch(p -> !p.isLeafPartition())) {
            throw new IllegalArgumentException("Must only specify leaf partitions with no parents");
        }
        Partition left = partitions.get(0);
        int numLeaves = partitions.size();
        for (int i = 1; i < numLeaves; i++) {
            Partition right = partitions.get(i);
            left = add(factory.parentJoining(UUID.randomUUID().toString(), left, right));
        }
        return this;
    }

    public PartitionsBuilder parentJoining(String parentId, String leftId, String rightId) {
        Partition left = partitionById(leftId);
        Partition right = partitionById(rightId);
        add(factory.parentJoining(parentId, left, right));
        return this;
    }

    public PartitionsBuilder rootFirst(String rootId) {
        add(factory.rootFirst(rootId));
        return this;
    }

    public PartitionsBuilder splitToNewChildrenOnDimension(
            String parentId, String leftId, String rightId, int dimension, String splitPoint) {
        Partition parent = partitionById(parentId);
        List<Partition> children = factory.split(parent, leftId, rightId, dimension, splitPoint);
        children.forEach(this::add);
        return this;
    }

    private Partition add(Partition partition) {
        partitions.add(partition);
        partitionById.put(partition.getId(), partition);
        return partition;
    }

    private Partition partitionById(String id) {
        return Optional.ofNullable(partitionById.get(id))
                .orElseThrow(() -> new IllegalArgumentException("Partition not specified: " + id));
    }

    public List<Partition> buildList() {
        return new ArrayList<>(partitions);
    }

    public PartitionTree buildTree() {
        return new PartitionTree(schema, partitions);
    }
}
