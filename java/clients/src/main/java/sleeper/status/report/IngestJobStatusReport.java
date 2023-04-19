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

package sleeper.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.console.ConsoleInput;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.job.common.QueueMessageCount;
import sleeper.status.report.ingest.job.IngestJobStatusReporter;
import sleeper.status.report.ingest.job.IngestQueueMessages;
import sleeper.status.report.ingest.job.JsonIngestJobStatusReporter;
import sleeper.status.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.status.report.job.query.JobQuery;
import sleeper.status.report.job.query.JobQueryArgument;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.util.ClientUtils.optionalArgument;

public class IngestJobStatusReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, IngestJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardIngestJobStatusReporter());
        REPORTERS.put("JSON", new JsonIngestJobStatusReporter());
    }

    private final IngestJobStatusStore statusStore;
    private final IngestJobStatusReporter ingestJobStatusReporter;
    private final QueueMessageCount.Client queueClient;
    private final InstanceProperties properties;
    private final JobQuery query;
    private final JobQuery.Type queryType;

    public IngestJobStatusReport(
            IngestJobStatusStore ingestJobStatusStore,
            String tableName, JobQuery.Type queryType, String queryParameters,
            IngestJobStatusReporter reporter, QueueMessageCount.Client queueClient, InstanceProperties properties) {
        this.statusStore = ingestJobStatusStore;
        this.query = JobQuery.fromParametersOrPrompt(tableName, queryType, queryParameters,
                Clock.systemUTC(), new ConsoleInput(System.console()));
        this.queryType = queryType;
        this.ingestJobStatusReporter = reporter;
        this.queueClient = queueClient;
        this.properties = properties;
    }

    public void run() {
        if (query == null) {
            return;
        }
        ingestJobStatusReporter.report(
                query.run(statusStore), queryType,
                IngestQueueMessages.from(properties, queueClient));
    }

    public static void main(String[] args) throws IOException {
        try {
            if (args.length < 2 || args.length > 5) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            String instanceId = args[0];
            String tableName = args[1];
            IngestJobStatusReporter reporter = getReporter(args, 2);
            JobQuery.Type queryType = JobQueryArgument.readTypeArgument(args, 3);
            String queryParameters = optionalArgument(args, 4).orElse(null);

            AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
            InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, instanceId);

            AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
            IngestJobStatusStore statusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
            AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
            new IngestJobStatusReport(statusStore, tableName, queryType, queryParameters,
                    reporter, QueueMessageCount.withSqsClient(sqsClient), instanceProperties).run();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("" +
                "Usage: <instance id> <table name> <report_type_standard_or_json> <optional_query_type> <optional_query_parameters> \n" +
                "Query types are:\n" +
                "-a (Return all jobs)\n" +
                "-d (Detailed, provide a jobId)\n" +
                "-r (Provide startRange and endRange separated by commas in format yyyyMMddhhmmss)\n" +
                "-u (Unfinished jobs)");
    }

    private static IngestJobStatusReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }
}
