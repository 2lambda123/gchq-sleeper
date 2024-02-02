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
package sleeper.systemtest.datageneration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.configuration.IngestMode;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;

import static sleeper.systemtest.configuration.IngestMode.BULK_IMPORT_QUEUE;
import static sleeper.systemtest.configuration.IngestMode.DIRECT;
import static sleeper.systemtest.configuration.IngestMode.GENERATE_ONLY;
import static sleeper.systemtest.configuration.IngestMode.QUEUE;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;

/**
 * Entrypoint for SystemTest image. Writes random data to Sleeper using the mechanism (ingestMode) defined in
 * the properties which were written to S3.
 */
public class IngestRandomData {

    private IngestRandomData() {
    }

    public static void main(String[] args) throws IOException {
        InstanceProperties instanceProperties;
        SystemTestPropertyValues systemTestProperties;
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        if (args.length == 2) {
            SystemTestProperties properties = new SystemTestProperties();
            properties.loadFromS3(s3Client, args[0]);
            instanceProperties = properties;
            systemTestProperties = properties.testPropertiesOnly();
        } else if (args.length == 3) {
            instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3(s3Client, args[0]);
            systemTestProperties = SystemTestStandaloneProperties.fromS3(s3Client, args[2]);
        } else {
            throw new RuntimeException("Wrong number of arguments detected. Usage: IngestRandomData <S3 bucket> <Table name> <optional system test bucket>");
        }
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        TableProperties tableProperties = new TablePropertiesProvider(instanceProperties, s3Client, dynamoClient)
                .getByName(args[1]);

        s3Client.shutdown();
        dynamoClient.shutdown();

        IngestMode ingestMode = systemTestProperties.getEnumValue(INGEST_MODE, IngestMode.class);
        if (ingestMode == QUEUE || ingestMode == BULK_IMPORT_QUEUE) {
            WriteRandomDataViaQueue.writeAndSendToQueue(ingestMode, instanceProperties, tableProperties, systemTestProperties);
        } else if (ingestMode == DIRECT) {
            StateStoreProvider stateStoreProvider = new StateStoreProvider(AmazonDynamoDBClientBuilder.defaultClient(),
                    instanceProperties, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
            WriteRandomDataDirect.writeWithIngestFactory(instanceProperties, tableProperties, systemTestProperties, stateStoreProvider);
        } else if (ingestMode == GENERATE_ONLY) {
            WriteRandomDataFiles.writeToS3GetDirectory(
                    instanceProperties, tableProperties, systemTestProperties,
                    WriteRandomData.createRecordIterator(systemTestProperties, tableProperties));
        } else {
            throw new IllegalArgumentException("Unrecognised ingest mode: " + ingestMode +
                    ". Only direct and queue ingest modes are available.");
        }
    }
}
