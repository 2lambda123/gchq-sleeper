/*
 * Copyright 2022 Crown Copyright
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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import sleeper.ClientUtils;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStore;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStore;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.status.report.compaction.job.CompactionJobStatusReportArguments;
import sleeper.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.status.report.compaction.task.CompactionTaskQuery;
import sleeper.status.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.status.report.job.query.JobQuery;
import sleeper.status.report.partitions.PartitionsStatusReporter;

import java.io.IOException;

import static sleeper.ClientUtils.optionalArgument;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * A utility class to report information about the partitions, the files, the
 * jobs, and the compaction tasks in the system.
 */
public class StatusReport {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final boolean verbose;
    private final StateStore stateStore;
    private final CompactionJobStatusStore compactionStatusStore;
    private final CompactionTaskStatusStore compactionTaskStatusStore;
    private final AmazonSQS sqsClient;
    private final TablePropertiesProvider tablePropertiesProvider;

    public StatusReport(InstanceProperties instanceProperties,
                        TableProperties tableProperties,
                        boolean verbose,
                        StateStore stateStore,
                        CompactionJobStatusStore compactionStatusStore,
                        CompactionTaskStatusStore compactionTaskStatusStore,
                        AmazonSQS sqsClient,
                        TablePropertiesProvider tablePropertiesProvider) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.verbose = verbose;
        this.stateStore = stateStore;
        this.compactionStatusStore = compactionStatusStore;
        this.compactionTaskStatusStore = compactionTaskStatusStore;
        this.sqsClient = sqsClient;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    private void run() throws StateStoreException {
        System.out.println("\nFull Status Report:\n--------------------------");
        // Partitions
        new PartitionsStatusReport(stateStore, tableProperties, new PartitionsStatusReporter(System.out)).run();

        // Data files
        new FilesStatusReport(stateStore, 1000, verbose).run();

        // Jobs
        new CompactionJobStatusReport(compactionStatusStore, CompactionJobStatusReportArguments.builder()
                .instanceId(instanceProperties.get(ID))
                .tableName(tableProperties.get(TABLE_NAME))
                .reporter(new StandardCompactionJobStatusReporter())
                .queryType(JobQuery.Type.UNFINISHED)
                .build()).run();

        // Tasks
        new CompactionTaskStatusReport(compactionTaskStatusStore,
                new StandardCompactionTaskStatusReporter(System.out),
                CompactionTaskQuery.UNFINISHED).run();

        // Dead letters
        new DeadLettersStatusReport(sqsClient, instanceProperties, tablePropertiesProvider).run();
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        if (2 != args.length && 3 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id> <table name> <optional_verbose_true_or_false>");
        }
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(amazonS3, instanceProperties);
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(args[1]);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, new Configuration());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        CompactionJobStatusStore compactionStatusStore = DynamoDBCompactionJobStatusStore.from(dynamoDBClient, instanceProperties);
        CompactionTaskStatusStore compactionTaskStatusStore = DynamoDBCompactionTaskStatusStore.from(dynamoDBClient, instanceProperties);

        boolean verbose = optionalArgument(args, 2)
                .map(Boolean::parseBoolean)
                .orElse(false);
        StatusReport statusReport = new StatusReport(
                instanceProperties, tableProperties, verbose,
                stateStore, compactionStatusStore, compactionTaskStatusStore,
                sqsClient, tablePropertiesProvider);
        amazonS3.shutdown();
        statusReport.run();
        sqsClient.shutdown();
        dynamoDBClient.shutdown();
    }
}
