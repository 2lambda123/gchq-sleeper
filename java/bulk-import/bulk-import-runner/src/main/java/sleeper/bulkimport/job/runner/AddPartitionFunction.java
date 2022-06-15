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
package sleeper.bulkimport.job.runner;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

/**
 * Given an {@link Iterator} of {@link Row}s, this class returns an {@link Iterator}
 * of {@link Row}s where each {@link Row} has a field added containing the id
 * of the partition that the key from the {@link Row} belongs to.
 */
public class AddPartitionFunction implements MapPartitionsFunction<Row, Row> {
    private static final long serialVersionUID = 4871009858051824361L;
    
    private final String schemaAsString;
    private final Broadcast<List<Partition>> broadcastPartitions;

    public AddPartitionFunction(String schemaAsString, Broadcast<List<Partition>> broadcastPartitions) {
        this.schemaAsString = schemaAsString;
        this.broadcastPartitions = broadcastPartitions;
    }
    
    @Override
    public Iterator<Row> call(Iterator<Row> input) throws Exception {
        Schema schema = new SchemaSerDe().fromJson(schemaAsString);
        List<Partition> partitions = broadcastPartitions.getValue();
        PartitionTree partitionTree = new PartitionTree(schema, partitions);
        return new AddPartitionIterator(input, schema, partitionTree);
    }
}
