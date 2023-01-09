/*
 * Copyright 2023 Crown Copyright
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

import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.schema.Schema;

import java.util.UUID;
import java.util.function.Consumer;

import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TablePropertiesTestHelper {

    private TablePropertiesTestHelper() {
    }

    public static TableProperties createTestTableProperties(
            InstanceProperties instanceProperties, Schema schema, AmazonS3 s3, Consumer<TableProperties> tableConfig) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableConfig.accept(tableProperties);
        try {
            s3.createBucket(tableProperties.get(DATA_BUCKET));
            tableProperties.saveToS3(s3);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to save table properties", e);
        }
        return tableProperties;
    }

    public static TableProperties createTestTableProperties(InstanceProperties instanceProperties, Schema schema) {
        String tableName = UUID.randomUUID().toString();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(schema);
        tableProperties.set(DATA_BUCKET, tableName + "-data");
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, tableName + "-af");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, tableName + "-rfgcf");
        tableProperties.set(PARTITION_TABLENAME, tableName + "-p");
        return tableProperties;
    }
}
