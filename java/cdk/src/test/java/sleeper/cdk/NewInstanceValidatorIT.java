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

package sleeper.cdk;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.statestore.s3.S3StateStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.ValidatorTestHelper.setupTablesPropertiesFile;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

@Testcontainers
public class NewInstanceValidatorIT {
    @Container
    public static final LocalStackContainer LOCALSTACK_CONTAINER
            = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    @TempDir
    public Path temporaryFolder;

    private static AmazonS3 amazonS3;
    private static AmazonDynamoDB amazonDynamoDB;
    private NewInstanceValidator newInstanceValidator;
    private final InstanceProperties instanceProperties = new InstanceProperties();

    @BeforeAll
    public static void setup() {
        amazonS3 = getS3Client();
        amazonDynamoDB = createDynamoClient();
    }

    @BeforeEach
    public void setUp() {
        newInstanceValidator = new NewInstanceValidator(amazonS3, amazonDynamoDB);
    }

    @Test
    public void shouldThrowAnErrorWhenABucketExistsWithSameNameAsTable() throws IOException {
        // Given
        String bucketName = String.join("-", "sleeper", "valid-id", "table", "example-table");
        instanceProperties.set(ID, "valid-id");
        setupTablesPropertiesFile(temporaryFolder, "example-table", "sleeper.statestore.dynamodb.DynamoDBStateStore");
        amazonS3.createBucket(bucketName);

        // When / Then
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sleeper table bucket exists: sleeper-valid-id-table-example-table");
        amazonS3.deleteBucket(bucketName);
    }

    @Test
    public void shouldThrowAnErrorWhenTheQueryResultsBucketExists() throws IOException {
        // Given
        String bucketName = String.join("-", "sleeper", "valid-id", "query-results");
        instanceProperties.set(ID, "valid-id");
        setupTablesPropertiesFile(temporaryFolder, "example-table", "sleeper.statestore.dynamodb.DynamoDBStateStore");
        amazonS3.createBucket(bucketName);

        // When / Then
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sleeper query results bucket exists: " + bucketName);
        amazonS3.deleteBucket(bucketName);
    }

    @Test
    public void shouldThrowAnErrorWhenDynamoTableExistsWithSameNameAsTableActiveFiles() throws IOException {
        checkErrorIsThrownWhenTableExists("sleeper-valid-id-table-example-table-active-files");
    }

    @Test
    public void shouldThrowAnErrorWhenADynamoTableExistsWithSameNameAsTableGCFiles() throws IOException {
        checkErrorIsThrownWhenTableExists("sleeper-valid-id-table-example-table-gc-files");
    }

    @Test
    public void shouldThrowAnErrorWhenADynamoTableExistsWithSameNameAsTablePartitions() throws IOException {
        checkErrorIsThrownWhenTableExists("sleeper-valid-id-table-example-table-partitions");
    }

    @Test
    public void checkNoErrorIsThrownWhenTableExistsButUsingS3StateStore() throws IOException {
        // Given
        String dynamoTable = "sleeper-valid-id-table-example-table-partitions";
        instanceProperties.set(ID, "valid-id");
        setupTablesPropertiesFile(temporaryFolder, "example-table", "sleeper.statestore.s3.S3StateStore");
        createDynamoTable(dynamoTable);

        // When
        assertThatCode(this::validate)
                .doesNotThrowAnyException();
        amazonDynamoDB.deleteTable(dynamoTable);
    }

    private void checkErrorIsThrownWhenTableExists(String dynamoTable) throws IOException {
        // Given
        instanceProperties.set(ID, "valid-id");
        setupTablesPropertiesFile(temporaryFolder, "example-table", "sleeper.statestore.dynamodb.DynamoDBStateStore");
        createDynamoTable(dynamoTable);

        // When
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sleeper DynamoDBTable exists: " + dynamoTable);
        amazonDynamoDB.deleteTable(dynamoTable);
    }

    private void validate() throws IOException {
        Path instancePropertiesPath = temporaryFolder.resolve("instance.properties");
        Files.writeString(instancePropertiesPath, instanceProperties.saveAsString());
        newInstanceValidator.validate(instanceProperties, instancePropertiesPath);
    }

    private static AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(LOCALSTACK_CONTAINER.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(LOCALSTACK_CONTAINER.getDefaultCredentialsProvider())
                .build();
    }

    protected static AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClient.builder()
                .withEndpointConfiguration(LOCALSTACK_CONTAINER.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .withCredentials(LOCALSTACK_CONTAINER.getDefaultCredentialsProvider())
                .build();
    }

    private void createDynamoTable(String tableName) {
        // These attributes are for the S3 state store, but for these tests it
        // doesn't matter if the attributes are correct for the DynamoDB state
        // store as we just need the table to exist.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(S3StateStore.REVISION_ID_KEY, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(new KeySchemaElement(S3StateStore.REVISION_ID_KEY, KeyType.HASH));
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withBillingMode(BillingMode.PAY_PER_REQUEST);
        amazonDynamoDB.createTable(request);

    }
}
