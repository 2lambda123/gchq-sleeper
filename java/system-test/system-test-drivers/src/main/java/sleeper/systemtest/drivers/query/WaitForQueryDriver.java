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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.PollWithRetries;
import sleeper.query.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.TrackedQuery;
import sleeper.query.tracker.exception.QueryTrackerException;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.time.Duration;

public class WaitForQueryDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForQueryDriver.class);
    private final DynamoDBQueryTracker queryTracker;
    private final PollWithRetries poll = PollWithRetries.intervalAndPollingTimeout(
            Duration.ofSeconds(10), Duration.ofMinutes(5));

    public WaitForQueryDriver(SleeperInstanceContext instance, AmazonDynamoDB dynamoDB) {
        this.queryTracker = new DynamoDBQueryTracker(instance.getInstanceProperties(), dynamoDB);
    }

    public void waitForQuery(String queryId) throws InterruptedException {
        poll.pollUntil("query is finished", () -> {
            try {
                TrackedQuery queryStatus = queryTracker.getStatus(queryId);
                if (queryStatus == null) {
                    LOGGER.info("Query not found yet, retrying...");
                    return false;
                }
                QueryState state = queryStatus.getLastKnownState();
                if (QueryState.FAILED.equals(state) || QueryState.PARTIALLY_FAILED.equals(state)) {
                    throw new IllegalStateException("Query failed: " + queryStatus);
                }
                LOGGER.info("Query found with state: {}", state);
                return QueryState.COMPLETED.equals(state);
            } catch (QueryTrackerException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
