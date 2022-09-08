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
package sleeper.athena.metadata;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import sleeper.athena.TestUtils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Field;
import sleeper.splitter.SplitPartition;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static sleeper.athena.metadata.IteratorApplyingMetadataHandler.MAX_ROW_KEY_PREFIX;
import static sleeper.athena.metadata.IteratorApplyingMetadataHandler.MIN_ROW_KEY_PREFIX;
import static sleeper.athena.metadata.SleeperMetadataHandler.RELEVANT_FILES_FIELD;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class IteratorApplyingMetadataHandlerIT extends AbstractMetadataHandlerIT {

    @Test
    public void shouldAddMinAndMaxRowKeysToTheRowsForEachLeafPartition() throws Exception {
        // Given
        InstanceProperties instance = TestUtils.createInstance(createS3Client());
        TableProperties table = createTable(instance);

        // When
        // Make query
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        IteratorApplyingMetadataHandler sleeperMetadataHandler = new IteratorApplyingMetadataHandler(
                s3Client, dynamoClient, instance.get(CONFIG_BUCKET),
                mock(EncryptionKeyFactory.class), mock(AWSSecretsManager.class), mock(AmazonAthena.class),
                "spillBucket", "spillPrefix");
        DynamoDBStateStore stateStore = new DynamoDBStateStore(table, dynamoClient);
        TableName tableName = new TableName(table.get(TABLE_NAME), table.get(TABLE_NAME));
        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName));
        GetTableLayoutRequest request = new GetTableLayoutRequest(TestUtils.createIdentity(),
                "abc",
                "def",
                tableName,
                new Constraints(new HashMap<>()),
                getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns()
        );
        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(),
                request);

        // Then
        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isEqualTo(4);
        for (int i = 0; i < leafPartitions.size(); i++) {
            Partition partition = leafPartitions.get(i);
            for (Field field : TIME_SERIES_SCHEMA.getRowKeyFields()) {
                String fieldName = MIN_ROW_KEY_PREFIX + "-" + field.getName();
                FieldReader reader = partitions.getFieldReader(fieldName);
                reader.setPosition(i);
                Object o = reader.readObject();
                assertThat(o).isEqualTo(partition.getRegion().getRange(field.getName()).getMin());

                fieldName = MAX_ROW_KEY_PREFIX + "-" + field.getName();
                reader = partitions.getFieldReader(fieldName);
                reader.setPosition(i);
                o = reader.readObject();
                assertThat(o).isEqualTo(partition.getRegion().getRange(field.getName()).getMax());
            }
        }
    }

    @Test
    public void shouldPassOnTheListofFilesAndRowKeysWhenCallingGetSplits() throws Exception {
        // Given
        InstanceProperties instance = TestUtils.createInstance(createS3Client());
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();

        // When
        IteratorApplyingMetadataHandler sleeperMetadataHandler = new IteratorApplyingMetadataHandler(s3Client, dynamoClient,
                instance.get(CONFIG_BUCKET),
                mock(EncryptionKeyFactory.class), mock(AWSSecretsManager.class), mock(AmazonAthena.class),
                "spillBucket", "spillPrefix");

        Block partitions = new BlockAllocatorImpl().createBlock(SchemaBuilder.newBuilder()
                .addIntField("_MinRowKey-year")
                .addStringField("_MinRowKey-month")
                .addIntField("_MaxRowKey-year")
                .addStringField("_MaxRowKey-month")
                .addStringField(RELEVANT_FILES_FIELD)
                .build());

        addPartition(partitions, 0, 25);
        addPartition(partitions, 1, 26);
        addPartition(partitions, 2, 27);

        GetSplitsResponse getSplitsResponse = sleeperMetadataHandler.doGetSplits(new BlockAllocatorImpl(),
                new GetSplitsRequest(TestUtils.createIdentity(), "abc", "def", new TableName("myDB", "myTable"),
                        partitions, new ArrayList<>(), new Constraints(new HashMap<>()), "unused"));

        // Then
        Set<Split> splits = getSplitsResponse.getSplits();
        assertThat(splits).hasSize(3);

        validateSplit(splits, 25);
        validateSplit(splits, 26);
        validateSplit(splits, 27);
    }

    @Test
    public void shouldIncludePartitionsWhenItHasBeenSplitBySystem() throws Exception {
        // Given
        InstanceProperties instance = TestUtils.createInstance(createS3Client());
        TableProperties table = createTable(instance);
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        IteratorApplyingMetadataHandler sleeperMetadataHandler = new IteratorApplyingMetadataHandler(s3Client, dynamoClient,
                instance.get(CONFIG_BUCKET),
                mock(EncryptionKeyFactory.class), mock(AWSSecretsManager.class), mock(AmazonAthena.class),
                "spillBucket", "spillPrefix");
        TableName tableName = new TableName(instance.get(ID), table.get(TABLE_NAME));

        // When
        DynamoDBStateStore stateStore = new DynamoDBStateStore(table, dynamoClient);
        Partition partition2018 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("year").getMin().equals(2018))
                .collect(Collectors.toList()).get(0);
        Map<String, List<String>> partitionToActiveFilesMap = stateStore.getPartitionToActiveFilesMap();
        SplitPartition splitPartition = new SplitPartition(stateStore, table.getSchema(), new Configuration());
        splitPartition.splitPartition(partition2018, partitionToActiveFilesMap.get(partition2018.getId()));

        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("year", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(2018).build());
        valueSets.put("month", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                5, true, 8, true)));

        Constraints queryConstraints = new Constraints(valueSets);
        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName));

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(),
                new GetTableLayoutRequest(TestUtils.createIdentity(),
                        "abc", "cde",
                        tableName,
                        queryConstraints, getTableResponse.getSchema(),
                        getTableResponse.getPartitionColumns()
                ));

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isEqualTo(2);
    }

    @Test
    public void shouldReturnLeftPartitionWhenItHasBeenSplitBySystemAndRangeMaxIsEqualToThePartitionMax() throws Exception {
        // Given
        InstanceProperties instance = TestUtils.createInstance(createS3Client());
        TableProperties table = createTable(instance);
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        IteratorApplyingMetadataHandler sleeperMetadataHandler = new IteratorApplyingMetadataHandler(s3Client, dynamoClient,
                instance.get(CONFIG_BUCKET),
                mock(EncryptionKeyFactory.class), mock(AWSSecretsManager.class), mock(AmazonAthena.class),
                "spillBucket", "spillPrefix");
        TableName tableName = new TableName(instance.get(ID), table.get(TABLE_NAME));

        // When
        DynamoDBStateStore stateStore = new DynamoDBStateStore(table, dynamoClient);
        Partition partition2018 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("year").getMin().equals(2018))
                .collect(Collectors.toList()).get(0);

        Map<String, List<String>> partitionToActiveFilesMap = stateStore.getPartitionToActiveFilesMap();
        SplitPartition splitPartition = new SplitPartition(stateStore, table.getSchema(), new Configuration());
        splitPartition.splitPartition(partition2018, partitionToActiveFilesMap.get(partition2018.getId()));

        Partition firstHalfOf2018 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("year").getMin().equals(2018))
                .filter(p -> p.getRegion().getRange("month").getMax() != null)
                .collect(Collectors.toList()).get(0);

        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("year", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(2018).build());
        valueSets.put("month", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                5, true, firstHalfOf2018.getRegion().getRange("month").getMax(), false)));

        Constraints queryConstraints = new Constraints(valueSets);
        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName));

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(), new GetTableLayoutRequest(
                TestUtils.createIdentity(),
                "abc", "cde",
                tableName,
                queryConstraints, getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns()
        ));

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isOne();
        FieldReader yearReader = partitions.getFieldReader("_MaxRowKey-year");
        FieldReader monthReader = partitions.getFieldReader("_MaxRowKey-month");
        FieldReader dayReader = partitions.getFieldReader("_MaxRowKey-day");
        assertThat(yearReader.readObject()).isEqualTo(2019);
        assertThat(monthReader.readObject()).isEqualTo(firstHalfOf2018.getRegion().getRange("month").getMax());
        assertThat(dayReader.readObject()).isNull();
    }

    @Test
    public void shouldAddRightPartitionWhenItHasBeenSplitBySystemAndRequestedValueEqualsToThePartitionMax() throws Exception {
        // Given
        InstanceProperties instance = TestUtils.createInstance(createS3Client());
        TableProperties table = createTable(instance);
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        IteratorApplyingMetadataHandler sleeperMetadataHandler = new IteratorApplyingMetadataHandler(s3Client, dynamoClient,
                instance.get(CONFIG_BUCKET),
                mock(EncryptionKeyFactory.class), mock(AWSSecretsManager.class), mock(AmazonAthena.class),
                "spillBucket", "spillPrefix");
        TableName tableName = new TableName(instance.get(ID), table.get(TABLE_NAME));

        // When
        DynamoDBStateStore stateStore = new DynamoDBStateStore(table, dynamoClient);
        Partition partition2018 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("year").getMin().equals(2018))
                .collect(Collectors.toList()).get(0);
        Map<String, List<String>> partitionToActiveFilesMap = stateStore.getPartitionToActiveFilesMap();
        SplitPartition splitPartition = new SplitPartition(stateStore, table.getSchema(), new Configuration());
        splitPartition.splitPartition(partition2018, partitionToActiveFilesMap.get(partition2018.getId()));

        Partition firstHalfOf2018 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("year").getMin().equals(2018))
                .filter(p -> p.getRegion().getRange("month").getMax() != null)
                .collect(Collectors.toList()).get(0);

        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("year", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(2018).build());
        valueSets.put("month", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(firstHalfOf2018.getRegion().getRange("month").getMax()).build());

        Constraints queryConstraints = new Constraints(valueSets);
        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName));

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(), new GetTableLayoutRequest(
                TestUtils.createIdentity(),
                "abc", "cde",
                tableName,
                queryConstraints, getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns()
        ));

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isOne();
        FieldReader yearReader = partitions.getFieldReader("_MinRowKey-year");
        FieldReader monthReader = partitions.getFieldReader("_MinRowKey-month");
        FieldReader dayReader = partitions.getFieldReader("_MinRowKey-day");
        assertThat(yearReader.readObject()).isEqualTo(2018);
        assertThat(monthReader.readObject()).isEqualTo(firstHalfOf2018.getRegion().getRange("month").getMax());
        assertThat(dayReader.readObject()).isEqualTo(Integer.MIN_VALUE);
    }

    private void validateSplit(Set<Split> splits, Integer expectedValue) {
        long matched = splits.stream()
                .filter(split -> split.getProperty("_MinRowKey-year").equals(expectedValue.toString()))
                .map(split -> {
                    assertThat(split.getProperty("_MaxRowKey-year")).isEqualTo(Integer.toString(expectedValue + 1));
                    assertThat(split.getProperty("_MinRowKey-month")).isEmpty();
                    assertThat(split.getProperty("_MaxRowKey-month")).isNull();
                    assertThat(split.getProperty(RELEVANT_FILES_FIELD)).isEqualTo("[\"s3a://table/partition-" + expectedValue + "/file1.parquet\"," +
                            "\"s3a://table/partition-" + expectedValue + "/file2.parquet\"]");
                    return split;
                })
                .count();

        if (matched != 1) {
            fail("No split in splits matched expected value: " + expectedValue + ". Splits: " + splits);
        }
    }

    private void addPartition(Block partitions, int position, int value) {
        partitions.setRowCount(partitions.getRowCount() + 1);
        BlockUtils.setValue(partitions.getFieldVector("_MinRowKey-year"), position, value);
        BlockUtils.setValue(partitions.getFieldVector("_MaxRowKey-year"), position, value + 1);
        BlockUtils.setValue(partitions.getFieldVector("_MinRowKey-month"), position, "");
        BlockUtils.setValue(partitions.getFieldVector("_MaxRowKey-month"), position, null);
        BlockUtils.setValue(partitions.getFieldVector(RELEVANT_FILES_FIELD), position, new Gson().toJson(Lists.newArrayList(
                "s3a://table/partition-" + value + "/file1.parquet",
                "s3a://table/partition-" + value + "/file2.parquet")));
    }
}
