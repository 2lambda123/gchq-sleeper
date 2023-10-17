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

package sleeper.configuration.properties.table;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableId;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperties.TABLES_PREFIX;

public class S3TablePropertiesStore implements TablePropertiesStore {

    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;

    public S3TablePropertiesStore(InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
    }

    @Override
    public TableProperties loadProperties(TableId tableId) {
        TableProperties properties = new TableProperties(instanceProperties);
        properties.loadFromS3(s3Client, tableId.getTableName());
        return properties;
    }

    @Override
    public Optional<TableProperties> loadByName(String tableName) {
        TableProperties properties = new TableProperties(instanceProperties);
        try {
            properties.loadFromS3(s3Client, tableName);
            return Optional.of(properties);
        } catch (AmazonS3Exception e) {
            if ("NoSuchKey".equals(e.getErrorCode())) {
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    @Override
    public Optional<TableProperties> loadByNameNoValidation(String tableName) {
        try {
            return Optional.of(new TableProperties(instanceProperties,
                    TableProperties.loadPropertiesFromS3(s3Client, instanceProperties, tableName)));
        } catch (AmazonS3Exception e) {
            if ("NoSuchKey".equals(e.getErrorCode())) {
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    @Override
    public Stream<TableProperties> streamAllTables() {
        return TableProperties.streamTablesFromS3(s3Client, dynamoClient, instanceProperties);
    }

    @Override
    public Stream<TableId> streamAllTableIds() {
        ListObjectsV2Result result = s3Client.listObjectsV2(instanceProperties.get(CONFIG_BUCKET), TableProperties.TABLES_PREFIX + "/");
        List<S3ObjectSummary> objectSummaries = result.getObjectSummaries();
        return objectSummaries.stream()
                .map(S3ObjectSummary::getKey)
                .map(s -> s.substring(TableProperties.TABLES_PREFIX.length() + 1))
                .map(tableName -> TableId.uniqueIdAndName(null, tableName));
    }

    @Override
    public void save(TableProperties tableProperties) {
        tableProperties.saveToS3(s3Client);
    }

    @Override
    public void deleteByName(String tableName) {
        s3Client.deleteObject(instanceProperties.get(CONFIG_BUCKET), TABLES_PREFIX + "/" + tableName);
    }
}
