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

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.console.ConsoleInput;
import sleeper.status.report.compaction.job.CompactionJobStatusReporter;
import sleeper.status.report.compaction.job.JsonCompactionJobStatusReporter;
import sleeper.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.status.report.job.query.JobQuery;
import sleeper.status.report.job.query.JobQueryArgument;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.util.ClientUtils.optionalArgument;

public class CompactionJobStatusReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, CompactionJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardCompactionJobStatusReporter());
        REPORTERS.put("JSON", new JsonCompactionJobStatusReporter());
    }

    private final CompactionJobStatusReporter compactionJobStatusReporter;
    private final CompactionJobStatusStore compactionJobStatusStore;
    private final JobQuery.Type queryType;
    private final JobQuery query;

    public CompactionJobStatusReport(
            CompactionJobStatusStore compactionJobStatusStore,
            CompactionJobStatusReporter reporter,
            String tableName, JobQuery.Type queryType) {
        this(compactionJobStatusStore, reporter, tableName, queryType, "");
    }

    public CompactionJobStatusReport(
            CompactionJobStatusStore compactionJobStatusStore,
            CompactionJobStatusReporter reporter,
            String tableName, JobQuery.Type queryType, String queryParameters) {
        this.compactionJobStatusStore = compactionJobStatusStore;
        this.compactionJobStatusReporter = reporter;
        this.query = JobQuery.fromParametersOrPrompt(tableName, queryType, queryParameters,
                Clock.systemUTC(), new ConsoleInput(System.console()));
        this.queryType = queryType;
    }

    public void run() {
        if (query == null) {
            return;
        }
        compactionJobStatusReporter.report(query.run(compactionJobStatusStore), queryType);
    }

    public static void main(String[] args) throws IOException {
        try {
            if (args.length < 2 || args.length > 5) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            String instanceId = args[0];
            String tableName = args[1];
            CompactionJobStatusReporter reporter = getReporter(args, 2);
            JobQuery.Type queryType = JobQueryArgument.readTypeArgument(args, 3);
            String queryParameters = optionalArgument(args, 4).orElse(null);

            AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
            InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, instanceId);

            AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
            CompactionJobStatusStore statusStore = DynamoDBCompactionJobStatusStore.from(dynamoDBClient, instanceProperties);
            new CompactionJobStatusReport(statusStore, reporter, tableName, queryType, queryParameters).run();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("" +
                "Usage: <instance id> <table name> <report_type_standard_or_json> <optional_query_type> <optional_query_parameters> \n" +
                "Query types are:\n" +
                "-a (Return all jobs)\n" +
                "-d (Detailed, provide a jobId)\n" +
                "-r (Provide startRange and endRange separated by commas in format yyyyMMddhhmmss)\n" +
                "-u (Unfinished jobs)");
    }

    private static CompactionJobStatusReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }
}
