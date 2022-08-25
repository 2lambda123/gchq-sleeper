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
package sleeper.cdk.custom;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TABLE_PROPERTIES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SleeperTableLambdaIT {
    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private static final Schema KEY_VALUE_SCHEMA = new Schema();

    static {
        KEY_VALUE_SCHEMA.setRowKeyFields(new Field("key", new StringType()));
        KEY_VALUE_SCHEMA.setValueFields(new Field("value", new LongType()));
    }

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private InstanceProperties initialiseInstance() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, "id");
        instanceProperties.set(JARS_BUCKET, "myJars");
        instanceProperties.set(CONFIG_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(REGION, "region");
        instanceProperties.set(VERSION, "1.2.3");
        instanceProperties.set(SUBNET, "subnet-12345");
        instanceProperties.set(VPC_ID, "vpc-12345");
        instanceProperties.set(ACCOUNT, "myaccount");
        instanceProperties.set(TABLE_PROPERTIES, "/path/to/table.properties");

        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3Client.shutdown();
        return instanceProperties;
    }

    @Test
    public void shouldInitialiseTheStateStoreWithNoSplitPointsOnCreate() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        InstanceProperties instanceProperties = initialiseInstance();
        TableProperties tableProperties = createTableProperties(instanceProperties);
        SleeperTableLambda sleeperTableLambda = new SleeperTableLambda(s3Client, dynamoClient);

        // When
        sleeperTableLambda.handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(createInput(instanceProperties, tableProperties))
                .build(), null);

        // Then
        Integer count = dynamoClient.scan(new ScanRequest().withTableName(tableProperties.get(PARTITION_TABLENAME)))
                .getCount();
        assertThat(count).isEqualTo(new Integer(1));
        s3Client.shutdown();
        dynamoClient.shutdown();
    }

    @Test
    public void shouldCreatePropertiesFileOnCreate() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        InstanceProperties instanceProperties = initialiseInstance();
        TableProperties tableProperties = createTableProperties(instanceProperties);
        SleeperTableLambda sleeperTableLambda = new SleeperTableLambda(s3Client, dynamoClient);

        // When
        sleeperTableLambda.handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(createInput(instanceProperties, tableProperties))
                .build(), null);

        // Then
        List<S3ObjectSummary> tables = s3Client.listObjectsV2(instanceProperties.get(CONFIG_BUCKET), "tables")
                .getObjectSummaries();
        assertThat(tables).hasSize(1);
        assertThat(tables.get(0).getKey()).isEqualTo("tables/" + tableProperties.get(TABLE_NAME));
        s3Client.shutdown();
        dynamoClient.shutdown();
    }

    @Test
    public void shouldDeletePropertiesFileOnDelete() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        InstanceProperties instanceProperties = initialiseInstance();
        TableProperties tableProperties = createTableProperties(instanceProperties);
        s3Client.putObject(instanceProperties.get(CONFIG_BUCKET),
                "tables/" + tableProperties.get(TABLE_NAME), "test");
        SleeperTableLambda sleeperTableLambda = new SleeperTableLambda(s3Client, dynamoClient);

        // When
        sleeperTableLambda.handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(createInput(instanceProperties, tableProperties))
                .build(), null);

        // Then
        List<S3ObjectSummary> tables = s3Client.listObjectsV2(instanceProperties.get(CONFIG_BUCKET), "tables")
                .getObjectSummaries();
        assertThat(tables).isEmpty();
        s3Client.shutdown();
        dynamoClient.shutdown();
    }

    @Test
    public void shouldUpdatePropertiesOnUpdate() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        InstanceProperties instanceProperties = initialiseInstance();
        TableProperties tableProperties = createTableProperties(instanceProperties);
        tableProperties.saveToS3(s3Client);
        SleeperTableLambda sleeperTableLambda = new SleeperTableLambda(s3Client, dynamoClient);

        // When
        tableProperties.set(ROW_GROUP_SIZE, "20");
        sleeperTableLambda.handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Update")
                .withResourceProperties(createInput(instanceProperties, tableProperties))
                .build(), null);

        // Then
        List<S3ObjectSummary> tables = s3Client.listObjectsV2(instanceProperties.get(CONFIG_BUCKET), "tables")
                .getObjectSummaries();
        assertThat(tables).hasSize(1);
        TableProperties downloaded = new TableProperties(instanceProperties);
        downloaded.loadFromS3(s3Client, tableProperties.get(TABLE_NAME));
        assertThat(downloaded.getInt(ROW_GROUP_SIZE)).isEqualTo(new Integer(20));
        s3Client.shutdown();
        dynamoClient.shutdown();
    }

    @Test
    public void shouldThrowExceptionIfRequestHasIncorrectType() {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        InstanceProperties instanceProperties = initialiseInstance();
        TableProperties tableProperties = createTableProperties(instanceProperties);
        SleeperTableLambda sleeperTableLambda = new SleeperTableLambda(s3Client, dynamoClient);

        // When / Then
        try {
            sleeperTableLambda.handleEvent(CloudFormationCustomResourceEvent.builder()
                    .withRequestType("RANDOM")
                    .withResourceProperties(createInput(instanceProperties, tableProperties))
                    .build(), null);
        } catch (Exception e) {
            assertThat(e.getMessage()).isEqualTo("Invalid request type: RANDOM");
        }
        s3Client.shutdown();
        dynamoClient.shutdown();
    }

    private TableProperties createTableProperties(InstanceProperties instanceProperties) {
        String tableName = UUID.randomUUID().toString();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        AmazonDynamoDB dynamoClient = createDynamoClient();
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket(tableName);
        s3Client.shutdown();
        try {
            new DynamoDBStateStoreCreator(tableName, KEY_VALUE_SCHEMA, dynamoClient)
                    .create();
        } catch (StateStoreException e) {
            throw new IllegalArgumentException("Could not create StateStore");
        }
        dynamoClient.shutdown();

        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(DATA_BUCKET, tableName);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, tableName + "-af");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, tableName + "rfgcf");
        tableProperties.set(PARTITION_TABLENAME, tableName + "-p");
        return tableProperties;
    }

    private Map<String, Object> createInput(InstanceProperties instanceProperties, TableProperties tableProperties) {
        Map<String, Object> input = new HashMap<>();
        try {
            input.put("instanceProperties", instanceProperties.saveAsString());
            input.put("tableProperties", tableProperties.saveAsString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save tableProperties");
        }

        return input;
    }
}
