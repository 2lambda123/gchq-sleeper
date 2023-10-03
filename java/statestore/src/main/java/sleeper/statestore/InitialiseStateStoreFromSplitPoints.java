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
package sleeper.statestore;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Initialises a {@link StateStore}. If a file of split points is
 * provided then these are used to create the initial {@link Partition}s.
 * Each line of the file should contain a single point which is a split in
 * the first dimension of the row key. (Only splitting by the first dimension
 * is supported.) If a file isn't provided then a single root {@link Partition}
 * is created.
 */
public class InitialiseStateStoreFromSplitPoints {
    private final AmazonDynamoDB dynamoDB;
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final List<Object> splitPoints;

    public InitialiseStateStoreFromSplitPoints(
            AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties,
            TableProperties tableProperties) throws IOException {
        this.dynamoDB = dynamoDB;
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        if (tableProperties.get(TableProperty.SPLIT_POINTS_FILE) != null) {
            this.splitPoints = readSplitPoints(tableProperties);
        } else {
            this.splitPoints = List.of();
        }
    }

    public InitialiseStateStoreFromSplitPoints(
            AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties,
            TableProperties tableProperties, List<Object> splitPoints) {
        this.dynamoDB = dynamoDB;
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.splitPoints = splitPoints;
    }

    public void run() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", DefaultAWSCredentialsProviderChain.class.getName());
        StateStore stateStore = new StateStoreFactory(dynamoDB, instanceProperties, conf).getStateStore(tableProperties);
        try {
            stateStore.initialise(new PartitionsFromSplitPoints(tableProperties.getSchema(), splitPoints).construct());
        } catch (StateStoreException e) {
            throw new RuntimeException("Failed to initialise State Store", e);
        }
    }

    public static void main(String[] args) throws StateStoreException, IOException {
        if (2 != args.length && 3 != args.length && 4 != args.length) {
            System.out.println("Usage: <Sleeper S3 Config Bucket> <Table name> <optional split points file> <optional boolean strings base64 encoded>");
            return;
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, args[0]);

        TableProperties tableProperties = new TablePropertiesProvider(s3Client, instanceProperties).getTableProperties(args[1]);

        List<Object> splitPoints = null;
        boolean stringsBase64Encoded = false;
        if (args.length > 2) {
            String splitPointsFile = args[2];
            stringsBase64Encoded = 4 == args.length && Boolean.parseBoolean(args[2]);
            splitPoints = readSplitPoints(tableProperties, splitPointsFile, stringsBase64Encoded);
        }

        new InitialiseStateStoreFromSplitPoints(dynamoDBClient, instanceProperties, tableProperties,
                splitPoints).run();

        dynamoDBClient.shutdown();
        s3Client.shutdown();
    }

    private static List<Object> readSplitPoints(TableProperties tableProperties) throws IOException {
        return readSplitPoints(tableProperties,
                tableProperties.get(TableProperty.SPLIT_POINTS_FILE),
                tableProperties.getBoolean(TableProperty.SPLIT_POINTS_BASE64_ENCODED));
    }

    private static List<Object> readSplitPoints(TableProperties tableProperties, String splitPointsFile, boolean stringsBase64Encoded) throws IOException {
        List<Object> splitPoints = new ArrayList<>();
        PrimitiveType rowKey1Type = tableProperties.getSchema().getRowKeyTypes().get(0);
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                Files.newInputStream(Paths.get(splitPointsFile)), StandardCharsets.UTF_8))) {
            String lineFromFile = reader.readLine();
            while (null != lineFromFile) {
                lines.add(lineFromFile);
                lineFromFile = reader.readLine();
            }
        }
        for (String line : lines) {
            if (rowKey1Type instanceof IntType) {
                splitPoints.add(Integer.parseInt(line));
            } else if (rowKey1Type instanceof LongType) {
                splitPoints.add(Long.parseLong(line));
            } else if (rowKey1Type instanceof StringType) {
                if (stringsBase64Encoded) {
                    byte[] encodedString = Base64.decodeBase64(line);
                    splitPoints.add(new String(encodedString, StandardCharsets.UTF_8));
                } else {
                    splitPoints.add(line);
                }
            } else if (rowKey1Type instanceof ByteArrayType) {
                splitPoints.add(Base64.decodeBase64(line));
            } else {
                throw new RuntimeException("Unknown key type " + rowKey1Type);
            }
        }
        System.out.println("Read " + splitPoints.size() + " split points from file");
        return splitPoints;
    }
}
