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

package sleeper.clients.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableWithNameAlreadyExistsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.table.TableIdentity.uniqueIdAndName;

@Testcontainers
public class RenameTableIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key1");
    private final TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDB);
    private final TablePropertiesStore propertiesStore = S3TableProperties.getStore(instanceProperties, s3, dynamoDB);

    @BeforeEach
    void setUp() {
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
    }

    @Test
    void shouldRenameExistingTableUsingTableIdentity() {
        // Given
        TableIdentity oldTableIdentity = uniqueIdAndName("table-1-id", "old-name");
        TableProperties oldProperties = createTable(oldTableIdentity);

        // When
        renameTable(oldTableIdentity, "new-name");

        // Then
        TableProperties expectedProperties = TableProperties.copyOf(oldProperties);
        expectedProperties.set(TABLE_NAME, "new-name");

        assertThat(tableIndex.getTableByName("new-name"))
                .get().isEqualTo(uniqueIdAndName("table-1-id", "new-name"));
        assertThat(tableIndex.getTableByName("old-name"))
                .isEmpty();
        assertThat(propertiesStore.loadByName("new-name"))
                .get().isEqualTo(expectedProperties);
        assertThat(propertiesStore.loadByName("old-name"))
                .isEmpty();
    }

    @Test
    void shouldRenameExistingTableUsingTableName() {
        // Given
        TableProperties oldProperties = createTable(uniqueIdAndName("table-1-id", "old-name"));

        // When
        renameTable("old-name", "new-name");

        // Then
        TableProperties expectedProperties = TableProperties.copyOf(oldProperties);
        expectedProperties.set(TABLE_NAME, "new-name");

        assertThat(tableIndex.getTableByName("new-name"))
                .get().isEqualTo(uniqueIdAndName("table-1-id", "new-name"));
        assertThat(tableIndex.getTableByName("old-name"))
                .isEmpty();
        assertThat(propertiesStore.loadByName("new-name"))
                .get().isEqualTo(expectedProperties);
        assertThat(propertiesStore.loadByName("old-name"))
                .isEmpty();
    }

    @Test
    void shouldFailToRenameTableWhichDoesNotExist() {
        // When / Then
        assertThatThrownBy(() -> renameTable("old-name", "new-name"))
                .isInstanceOf(TableNotFoundException.class);
    }

    @Test
    void shouldFailToRenameTableWhenTableWithNewNameAlreadyExists() {
        // Given
        createTable(uniqueIdAndName("table-1-id", "table-1"));
        createTable(uniqueIdAndName("table-2-id", "table-2"));

        // When / Then
        assertThatThrownBy(() -> renameTable("table-1", "table-2"))
                .isInstanceOf(TableWithNameAlreadyExistsException.class);
    }

    private void renameTable(String oldName, String newName) {
        new RenameTable(tableIndex, propertiesStore).rename(oldName, newName);
    }

    private void renameTable(TableIdentity tableIdentity, String newName) {
        new RenameTable(tableIndex, propertiesStore).rename(tableIdentity, newName);
    }

    private TableProperties createTable(TableIdentity tableIdentity) {
        TableProperties table = createTestTableProperties(instanceProperties, schema);
        table.set(TABLE_ID, tableIdentity.getTableUniqueId());
        table.set(TABLE_NAME, tableIdentity.getTableName());
        tableIndex.create(tableIdentity);
        propertiesStore.save(table);
        return table;
    }
}
