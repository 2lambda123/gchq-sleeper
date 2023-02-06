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

package sleeper.configuration.properties.local;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class SaveLocalProperties {
    private final InstanceProperties instanceProperties;
    private final List<TableProperties> tables;

    private SaveLocalProperties(InstanceProperties instanceProperties, List<TableProperties> tables) {
        this.instanceProperties = instanceProperties;
        this.tables = tables;
    }

    public static SaveLocalProperties loadFromS3(AmazonS3 s3, String instanceId) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        try {
            instanceProperties.loadFromS3GivenInstanceId(s3, instanceId);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new SaveLocalProperties(instanceProperties,
                loadTablesFromS3(s3, instanceProperties).collect(Collectors.toList()));
    }

    public static SaveLocalProperties loadFromPath(Path path) {
        InstanceProperties instanceProperties = new InstanceProperties();
        try {
            instanceProperties.load(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new SaveLocalProperties(instanceProperties, null);
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public String getConfigBucket() {
        return instanceProperties.get(CONFIG_BUCKET);
    }

    public String getQueryResultsBucket() {
        return instanceProperties.get(QUERY_RESULTS_BUCKET);
    }

    public String getTags() throws IOException {
        return instanceProperties.getTagsPropertiesAsString();
    }

    public List<TableProperties> getTables() {
        return tables;
    }

    private static Stream<TableProperties> loadTablesFromS3(AmazonS3 s3, InstanceProperties instanceProperties) {
        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        return s3.listObjectsV2(configBucket, "tables/")
                .getObjectSummaries().stream()
                .map(tableConfigObject -> loadTableFromS3(s3, instanceProperties, tableConfigObject));
    }

    private static TableProperties loadTableFromS3(
            AmazonS3 s3, InstanceProperties instanceProperties, S3ObjectSummary tableConfigObject) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        try (InputStream in = s3.getObject(
                        tableConfigObject.getBucketName(),
                        tableConfigObject.getKey())
                .getObjectContent()) {
            tableProperties.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return tableProperties;
    }
}
