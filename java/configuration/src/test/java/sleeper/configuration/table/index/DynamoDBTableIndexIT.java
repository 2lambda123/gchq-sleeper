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

package sleeper.configuration.table.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableNotFoundException;
import sleeper.dynamodb.tools.DynamoDBTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;

public class DynamoDBTableIndexIT extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final DynamoDBTableIndex index = new DynamoDBTableIndex(instanceProperties, dynamoDBClient);
    private final TableIdGenerator idGenerator = new TableIdGenerator();

    @BeforeEach
    void setUp() {
        DynamoDBTableIndexCreator.create(dynamoDBClient, instanceProperties);
    }

    @Nested
    @DisplayName("Create a table")
    class CreateTable {
        @Test
        void shouldCreateATable() {
            TableIdentity tableId = createTable("test-table");

            assertThat(index.streamAllTables())
                    .containsExactly(tableId);
        }

        @Test
        void shouldFailToCreateATableWhichAlreadyExists() {
            createTable("duplicate-table");

            assertThatThrownBy(() -> createTable("duplicate-table"))
                    .isInstanceOf(TableAlreadyExistsException.class);
        }
    }

    @Nested
    @DisplayName("Look up a table")
    class LookupTable {

        @Test
        void shouldGetTableByName() {
            TableIdentity tableId = createTable("test-table");

            assertThat(index.getTableByName("test-table"))
                    .contains(tableId);
        }

        @Test
        void shouldGetNoTableByName() {
            createTable("existing-table");

            assertThat(index.getTableByName("not-a-table"))
                    .isEmpty();
        }

        @Test
        void shouldGetTableById() {
            TableIdentity tableId = createTable("test-table");

            assertThat(index.getTableByUniqueId(tableId.getTableUniqueId()))
                    .contains(tableId);
        }

        @Test
        void shouldGetNoTableById() {
            createTable("existing-table");

            assertThat(index.getTableByUniqueId("not-a-table"))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("List tables")
    class ListTables {

        @Test
        void shouldGetTablesOrderedByName() {
            createTable("some-table");
            createTable("a-table");
            createTable("this-table");
            createTable("other-table");

            assertThat(index.streamAllTables())
                    .extracting(TableIdentity::getTableName)
                    .containsExactly(
                            "a-table",
                            "other-table",
                            "some-table",
                            "this-table");
        }

        @Test
        void shouldGetTableIds() {
            TableIdentity table1 = createTable("first-table");
            TableIdentity table2 = createTable("second-table");

            assertThat(index.streamAllTables())
                    .containsExactly(table1, table2);
        }

        @Test
        void shouldGetNoTables() {
            assertThat(index.streamAllTables()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Delete table")
    class DeleteTable {

        @Test
        void deleteTableNameReference() {
            TableIdentity tableId = createTable("test-table");

            index.delete(tableId);

            assertThat(index.getTableByName("test-table")).isEmpty();
        }

        @Test
        void deleteTableIdReference() {
            TableIdentity tableId = createTable("test-table");

            index.delete(tableId);

            assertThat(index.getTableByUniqueId(tableId.getTableUniqueId())).isEmpty();
        }

        @Test
        void shouldFailToDeleteTableThatDoesNotExist() {
            // Given
            TableIdentity tableId = TableIdentity.uniqueIdAndName("not-a-table-id", "not-a-table");

            // When / Then
            assertThatThrownBy(() -> index.delete(tableId))
                    .isInstanceOf(TableNotFoundException.class);
        }

        @Test
        void shouldFailToDeleteTableIfTableRenamedAfterLoadingOldId() {
            // Given
            TableIdentity oldId = TableIdentity.uniqueIdAndName("test-id", "old-name");
            TableIdentity renamedId = TableIdentity.uniqueIdAndName("test-id", "changed-name");
            index.create(oldId);
            index.update(renamedId);

            // When/Then
            assertThatThrownBy(() -> index.delete(oldId))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).contains(renamedId);
        }

        @Test
        void shouldFailToDeleteTableIfTableDeletedAndRecreatedAfterLoadingOldId() {
            // Given
            TableIdentity oldId = TableIdentity.uniqueIdAndName("test-id-1", "table-name");
            TableIdentity recreatedId = TableIdentity.uniqueIdAndName("test-id-2", "table-name");
            index.create(recreatedId);

            // When/Then
            assertThatThrownBy(() -> index.delete(oldId))
                    .isInstanceOf(TableNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Update table")
    class UpdateTable {
        @Test
        void shouldUpdateTableName() {
            // Given
            TableIdentity tableId = createTable("old-name");

            // When
            TableIdentity newTableId = TableIdentity.uniqueIdAndName(tableId.getTableUniqueId(), "new-name");
            index.update(newTableId);

            // Then
            assertThat(index.streamAllTables())
                    .containsExactly(newTableId);
            assertThat(index.getTableByName("new-name"))
                    .contains(newTableId);
            assertThat(index.getTableByName("old-name")).isEmpty();
            assertThat(index.getTableByUniqueId(newTableId.getTableUniqueId()))
                    .contains(newTableId);
        }

        @Test
        void shouldFailToUpdateTableIfTableDoesNotExist() {
            // Given
            TableIdentity newTableId = TableIdentity.uniqueIdAndName("not-a-table-id", "new-name");

            // When/Then
            assertThatThrownBy(() -> index.update(newTableId))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).isEmpty();
        }

        @Test
        void shouldFailToUpdateTableIfTableDeletedAfterLoadingOldId() {
            // Given
            TableIdentity oldId = TableIdentity.uniqueIdAndName("test-id", "old-name");
            TableIdentity newId = TableIdentity.uniqueIdAndName("test-id", "new-name");

            // When/Then
            assertThatThrownBy(() -> index.update(oldId, newId))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).isEmpty();
        }

        @Test
        void shouldFailToUpdateTableIfTableRenamedAfterLoadingOldId() {
            // Given
            TableIdentity oldId = TableIdentity.uniqueIdAndName("test-id", "old-name");
            TableIdentity renamedId = TableIdentity.uniqueIdAndName("test-id", "changed-name");
            TableIdentity newId = TableIdentity.uniqueIdAndName("test-id", "new-name");
            index.create(oldId);
            index.update(renamedId);

            // When/Then
            assertThatThrownBy(() -> index.update(oldId, newId))
                    .isInstanceOf(TableNotFoundException.class);
            assertThat(index.streamAllTables()).contains(renamedId);
        }

        @Test
        void shouldFailToUpdateTableIfTableWithSameNameAlreadyExists() {
            // Given
            createTable("test-name-1");
            TableIdentity tableId2 = createTable("test-name-2");

            // When / Then
            TableIdentity newTableId = TableIdentity.uniqueIdAndName(tableId2.getTableUniqueId(), "test-name-1");
            assertThatThrownBy(() -> index.update(newTableId))
                    .isInstanceOf(TableAlreadyExistsException.class);
        }
    }

    private TableIdentity createTable(String tableName) {
        TableIdentity tableId = TableIdentity.uniqueIdAndName(idGenerator.generateString(), tableName);
        index.create(tableId);
        return tableId;
    }
}
