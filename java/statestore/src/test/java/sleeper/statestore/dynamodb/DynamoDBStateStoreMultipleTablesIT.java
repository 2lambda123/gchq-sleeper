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

package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class DynamoDBStateStoreMultipleTablesIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;
    @Container
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key", new LongType());
    private final StateStoreFactory stateStoreFactory = new StateStoreFactory(dynamoDBClient, instanceProperties, new Configuration());
    private final FileInfoFactory fileInfoFactory = FileInfoFactory.builder().schema(schema)
            .partitionTree(new PartitionsBuilder(schema).singlePartition("root").buildTree())
            .build();

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, DYNAMO_PORT, AmazonDynamoDBClientBuilder.standard());
    }

    @AfterAll
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    @BeforeEach
    void setUp() {
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();
    }

    private StateStore createTableStateStore() throws Exception {
        StateStore stateStore = stateStoreFactory.getStateStore(
                createTestTableProperties(instanceProperties, schema));
        stateStore.initialise();
        return stateStore;
    }

    @Test
    @Disabled("TODO")
    void shouldCreateOneStateStoreWithTwoTables() throws Exception {
        // Given
        StateStore stateStore1 = createTableStateStore();
        StateStore stateStore2 = createTableStateStore();
        FileInfo file1 = fileInfoFactory.leafFile("file1.parquet", 12, 1L, 12L);
        FileInfo file2 = fileInfoFactory.leafFile("file1.parquet", 34, 10L, 20L);

        // When
        stateStore1.addFile(file1);
        stateStore2.addFile(file2);

        // Then
        assertThat(stateStore1.getActiveFiles()).containsExactly(file1);
        assertThat(stateStore2.getActiveFiles()).containsExactly(file2);
    }
}
