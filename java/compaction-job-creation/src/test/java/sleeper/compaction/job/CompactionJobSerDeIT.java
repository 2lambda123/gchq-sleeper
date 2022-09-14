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
package sleeper.compaction.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.lang3.tuple.MutablePair;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.table.job.TableCreator;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class CompactionJobSerDeIT {
    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB
    );

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

    private InstanceProperties createInstanceProperties(AmazonS3 s3) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(CONFIG_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(FILE_SYSTEM, "");

        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));

        return instanceProperties;
    }

    private void createTable(AmazonS3 s3, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties, String tableName, Schema schema) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(schema);
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "2");
        TableCreator tableCreator = new TableCreator(s3, dynamoDB, instanceProperties);
        tableCreator.createTable(tableProperties);
    }

    private Schema schemaWithStringKey() {
        return Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    }

    private Schema schemaWith2StringKeysAndOneOfType(PrimitiveType type) {
        return Schema.builder()
                .rowKeyFields(
                        new Field("key1", new StringType()),
                        new Field("key2", new StringType()),
                        new Field("key3", type))
                .build();
    }

    @Test
    public void shouldSerDeCorrectlyForNonSplittingJobWithNoIterator() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        String tableName = UUID.randomUUID().toString();
        CompactionJob compactionJob = new CompactionJob(tableName, "compactionJob-1");
        compactionJob.setInputFiles(Arrays.asList("file1", "file2"));
        compactionJob.setOutputFile("outputfile");
        compactionJob.setPartitionId("partition1");
        compactionJob.setIsSplittingJob(false);
        Schema schema = schemaWithStringKey();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        createTable(s3Client, dynamoDBClient, instanceProperties, tableName, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }

    @Test
    public void shouldSerDeCorrectlyForNonSplittingJobWithIterator() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        String tableName = UUID.randomUUID().toString();
        CompactionJob compactionJob = new CompactionJob(tableName, "compactionJob-1");
        compactionJob.setInputFiles(Arrays.asList("file1", "file2"));
        compactionJob.setOutputFile("outputfile");
        compactionJob.setPartitionId("partition1");
        compactionJob.setIsSplittingJob(false);
        compactionJob.setIteratorClassName("Iterator.class");
        compactionJob.setIteratorConfig("config1");
        Schema schema = schemaWithStringKey();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        createTable(s3Client, dynamoDBClient, instanceProperties, tableName, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobStringKeyWithNoIterator() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        String tableName = UUID.randomUUID().toString();
        CompactionJob compactionJob = new CompactionJob(tableName, "compactionJob-1");
        compactionJob.setInputFiles(Arrays.asList("file1", "file2"));
        compactionJob.setOutputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"));
        compactionJob.setPartitionId("partition1");
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint("G");
        compactionJob.setDimension(2);
        compactionJob.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
        Schema schema = schemaWith2StringKeysAndOneOfType(new StringType());
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        createTable(s3Client, dynamoDBClient, instanceProperties, tableName, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobIntKeyWithIterator() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        String tableName = UUID.randomUUID().toString();
        CompactionJob compactionJob = new CompactionJob(tableName, "compactionJob-1");
        compactionJob.setInputFiles(Arrays.asList("file1", "file2"));
        compactionJob.setOutputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"));
        compactionJob.setPartitionId("partition1");
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint(10);
        compactionJob.setIteratorClassName("Iterator.class");
        compactionJob.setIteratorConfig("config1");
        compactionJob.setDimension(2);
        compactionJob.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
        Schema schema = schemaWith2StringKeysAndOneOfType(new IntType());
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        createTable(s3Client, dynamoDBClient, instanceProperties, tableName, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobLongKeyWithIterator() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        String tableName = UUID.randomUUID().toString();
        CompactionJob compactionJob = new CompactionJob(tableName, "compactionJob-1");
        compactionJob.setInputFiles(Arrays.asList("file1", "file2"));
        compactionJob.setOutputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"));
        compactionJob.setPartitionId("partition1");
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint(10L);
        compactionJob.setIteratorClassName("Iterator.class");
        compactionJob.setIteratorConfig("config1");
        compactionJob.setDimension(2);
        compactionJob.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
        Schema schema = schemaWith2StringKeysAndOneOfType(new LongType());
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        createTable(s3Client, dynamoDBClient, instanceProperties, tableName, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobStringKeyWithIterator() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        String tableName = UUID.randomUUID().toString();
        CompactionJob compactionJob = new CompactionJob(tableName, "compactionJob-1");
        compactionJob.setInputFiles(Arrays.asList("file1", "file2"));
        compactionJob.setOutputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"));
        compactionJob.setPartitionId("partition1");
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint("G");
        compactionJob.setIteratorClassName("Iterator.class");
        compactionJob.setIteratorConfig("config1");
        compactionJob.setDimension(2);
        compactionJob.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
        Schema schema = schemaWith2StringKeysAndOneOfType(new StringType());
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        createTable(s3Client, dynamoDBClient, instanceProperties, tableName, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobByteArrayKeyWithIterator() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        String tableName = UUID.randomUUID().toString();
        CompactionJob compactionJob = new CompactionJob(tableName, "compactionJob-1");
        compactionJob.setInputFiles(Arrays.asList("file1", "file2"));
        compactionJob.setOutputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"));
        compactionJob.setPartitionId("partition1");
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint(new byte[]{1, 2, 4, 8});
        compactionJob.setIteratorClassName("Iterator.class");
        compactionJob.setIteratorConfig("config1");
        compactionJob.setDimension(2);
        compactionJob.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
        Schema schema = schemaWith2StringKeysAndOneOfType(new ByteArrayType());
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        createTable(s3Client, dynamoDBClient, instanceProperties, tableName, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }
}
