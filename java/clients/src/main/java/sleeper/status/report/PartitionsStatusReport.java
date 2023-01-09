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
package sleeper.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.status.report.partitions.PartitionsStatus;
import sleeper.status.report.partitions.PartitionsStatusReportArguments;
import sleeper.status.report.partitions.PartitionsStatusReporter;

import java.io.IOException;

/**
 * A utility class to report information about the partitions in the system and
 * their status.
 */
public class PartitionsStatusReport {
    private final StateStore store;
    private final TableProperties tableProperties;
    private final PartitionsStatusReporter reporter;

    public PartitionsStatusReport(StateStore store, TableProperties tableProperties, PartitionsStatusReporter reporter) {
        this.store = store;
        this.tableProperties = tableProperties;
        this.reporter = reporter;
    }

    public void run() throws StateStoreException {
        reporter.report(PartitionsStatus.from(tableProperties, store));
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        PartitionsStatusReportArguments arguments;
        try {
            arguments = PartitionsStatusReportArguments.fromArgs(args);
        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            PartitionsStatusReportArguments.printUsage(System.err);
            System.exit(1);
            return;
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        arguments.runReport(amazonS3, dynamoDBClient, System.out);
        amazonS3.shutdown();
        dynamoDBClient.shutdown();
    }
}
