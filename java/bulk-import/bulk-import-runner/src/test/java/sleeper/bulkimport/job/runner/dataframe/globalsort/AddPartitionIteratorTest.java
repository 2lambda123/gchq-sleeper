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
package sleeper.bulkimport.job.runner.dataframe.globalsort;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Test;

import sleeper.bulkimport.job.runner.dataframe.globalsort.AddPartitionIterator;
import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AddPartitionIteratorTest {

    @Test
    public void shouldAddPartitionField() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();
        Row row1 = RowFactory.create(1, 2L, "3");
        Row row2 = RowFactory.create(4, 5L, "6");
        Iterator<Row> rows = Arrays.asList(row1, row2).iterator();
        List<Object> splitPoints = Collections.singletonList(3);
        PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);
        PartitionTree partitionTree = new PartitionTree(schema, partitionsFromSplitPoints.construct());
        String partition1 = partitionTree.getLeafPartition(Key.create(1)).getId();
        String partition2 = partitionTree.getLeafPartition(Key.create(4)).getId();
        AddPartitionIterator addPartitionIterator = new AddPartitionIterator(rows, schema, partitionTree);

        // When / Then
        Row expectedRow1 = RowFactory.create(1, 2L, "3", partition1);
        Row expectedRow2 = RowFactory.create(4, 5L, "6", partition2);
        assertThat(addPartitionIterator).toIterable()
                .containsExactly(expectedRow1, expectedRow2);
    }
}
