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

package sleeper.systemtest.drivers.query;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.util.PollWithRetries;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.query.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.QueryTrackerStore;
import sleeper.query.tracker.TrackedQuery;
import sleeper.query.tracker.exception.QueryTrackerException;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.util.ReadRecordsFromS3;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class SQSQueryDriver implements QueryDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQSQueryDriver.class);

    private final SleeperInstanceContext instance;
    private final AmazonSQS sqsClient;
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonS3 s3Client;
    private final PollWithRetries poll = PollWithRetries.intervalAndPollingTimeout(
            Duration.ofSeconds(2), Duration.ofMinutes(1));

    public SQSQueryDriver(SleeperInstanceContext instance,
                          AmazonSQS sqsClient,
                          AmazonDynamoDB dynamoDBClient,
                          AmazonS3 s3Client) {
        this.instance = instance;
        this.sqsClient = sqsClient;
        this.dynamoDBClient = dynamoDBClient;
        this.s3Client = s3Client;
    }

    public List<Record> run(Query query) throws InterruptedException {
        send(query);
        waitForQuery(query);
        return getResults(query);
    }

    public void send(Query query) {
        sqsClient.sendMessage(
                instance.getInstanceProperties().get(QUERY_QUEUE_URL),
                new QuerySerDe(instance.getTablePropertiesProvider()).toJson(query));
    }

    public void waitForQuery(Query query) throws InterruptedException {
        QueryTrackerStore queryTracker = new DynamoDBQueryTracker(instance.getInstanceProperties(), dynamoDBClient);
        poll.pollUntil("query is finished", () -> {
            try {
                TrackedQuery queryStatus = queryTracker.getStatus(query.getQueryId());
                if (queryStatus == null) {
                    LOGGER.info("Query not found yet, retrying...");
                    return false;
                }
                QueryState state = queryStatus.getLastKnownState();
                if (QueryState.FAILED == state || QueryState.PARTIALLY_FAILED == state) {
                    throw new IllegalStateException("Query failed: " + queryStatus);
                }
                LOGGER.info("Query found with state: {}", state);
                return QueryState.COMPLETED == state;
            } catch (QueryTrackerException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public List<Record> getResults(Query query) {
        Schema schema = instance.getTablePropertiesByName(query.getTableName()).orElseThrow().getSchema();
        return s3Client.listObjects(
                        instance.getInstanceProperties().get(QUERY_RESULTS_BUCKET),
                        "query-" + query.getQueryId())
                .getObjectSummaries().stream()
                .flatMap(object -> ReadRecordsFromS3.getRecords(schema, object))
                .collect(Collectors.toUnmodifiableList());
    }
}
