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

package sleeper.configuration.properties.table;

import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TablePropertiesStore {

    private static final TableIdGenerator ID_GENERATOR = new TableIdGenerator();

    private final TableIndex tableIndex;
    private final Client client;

    public TablePropertiesStore(TableIndex tableIndex, Client client) {
        this.tableIndex = tableIndex;
        this.client = client;
    }

    public TableProperties loadProperties(TableIdentity tableId) {
        TableProperties tableProperties = client.loadProperties(tableId);
        tableProperties.validate();
        return tableProperties;
    }

    public TableProperties findByName(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(this::loadProperties)
                .orElseThrow(() -> TableNotFoundException.withTableName(tableName));
    }

    public Optional<TableProperties> loadById(String tableId) {
        return tableIndex.getTableByUniqueId(tableId)
                .map(this::loadProperties);
    }

    public TableProperties findByNameNoValidation(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(client::loadProperties)
                .orElseThrow(() -> TableNotFoundException.withTableName(tableName));
    }

    public Stream<TableProperties> streamAllTables() {
        return streamAllTableIds().map(this::loadProperties);
    }

    public Stream<TableIdentity> streamAllTableIds() {
        return tableIndex.streamAllTables();
    }

    public void createTable(TableProperties tableProperties) {
        String tableName = tableProperties.get(TableProperty.TABLE_NAME);
        tableIndex.getTableByName(tableName).ifPresent(tableId -> {
            throw new TableAlreadyExistsException(tableId);
        });
        createWhenNotInIndex(tableProperties);
    }

    public void save(TableProperties tableProperties) {
        Optional<TableIdentity> existingId = getExistingId(tableProperties);
        if (existingId.isPresent()) {
            TableIdentity id = existingId.get();
            String tableName = tableProperties.get(TABLE_NAME);
            if (!Objects.equals(id.getTableName(), tableName)) {
                tableIndex.update(TableIdentity.uniqueIdAndName(id.getTableUniqueId(), tableName));
            }
            tableProperties.set(TABLE_ID, id.getTableUniqueId());
            client.saveProperties(tableProperties);
        } else {
            createWhenNotInIndex(tableProperties);
        }
    }

    private Optional<TableIdentity> getExistingId(TableProperties tableProperties) {
        if (tableProperties.isSet(TABLE_ID)) {
            return tableIndex.getTableByUniqueId(tableProperties.get(TABLE_ID));
        } else {
            return tableIndex.getTableByName(tableProperties.get(TABLE_NAME));
        }
    }

    private void createWhenNotInIndex(TableProperties tableProperties) {
        if (!tableProperties.isSet(TABLE_ID)) {
            tableProperties.set(TABLE_ID, ID_GENERATOR.generateString());
        }
        client.saveProperties(tableProperties);
        tableIndex.create(tableProperties.getId());
    }

    public void deleteByName(String tableName) {
        tableIndex.getTableByName(tableName)
                .ifPresent(this::delete);
    }

    public void delete(TableIdentity tableId) {
        tableIndex.delete(tableId);
        client.deleteProperties(tableId);
    }

    public interface Client {
        TableProperties loadProperties(TableIdentity tableId);

        void saveProperties(TableProperties tableProperties);

        void deleteProperties(TableIdentity tableId);
    }
}
