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

package sleeper.ingest.status.store.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableIdentity;
import sleeper.ingest.IngestStatusStoreException;
import sleeper.ingest.job.status.IngestJobFinishedEvent;
import sleeper.ingest.job.status.IngestJobStartedEvent;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobValidatedEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;
import static sleeper.ingest.job.status.IngestJobStatusType.REJECTED;

public class DynamoDBIngestJobStatusStore implements IngestJobStatusStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestJobStatusStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String statusTableName;
    private final DynamoDBIngestJobStatusFormat format;

    DynamoDBIngestJobStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties, Supplier<Instant> getTimeNow) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = jobStatusTableName(properties.get(ID));
        int timeToLiveInSeconds = properties.getInt(INGEST_JOB_STATUS_TTL_IN_SECONDS);
        this.format = new DynamoDBIngestJobStatusFormat(timeToLiveInSeconds, getTimeNow);
    }

    public static String jobStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-job-status");
    }

    @Override
    public void jobValidated(IngestJobValidatedEvent event) {
        try {
            PutItemResult result = putItem(format.createJobValidatedRecord(event));
            LOGGER.info("Put validated event for job {} to table {}, capacity consumed = {}",
                    event.getJobId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed putItem in jobValidated for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobStarted(IngestJobStartedEvent event) {
        try {
            PutItemResult result = putItem(format.createJobStartedRecord(event));
            LOGGER.info("Put started event for job {} to table {}, capacity consumed = {}",
                    event.getJobId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed putItem in jobStarted for job " + event.getJobId(), e);
        }
    }

    @Override
    public void jobFinished(IngestJobFinishedEvent event) {
        try {
            PutItemResult result = putItem(format.createJobFinishedRecord(event));
            LOGGER.info("Put finished event for job {} to table {}, capacity consumed = {}",
                    event.getJobId(), statusTableName, result.getConsumedCapacity().getCapacityUnits());
        } catch (RuntimeException e) {
            throw new IngestStatusStoreException("Failed putItem in jobFinished for job " + event.getJobId(), e);
        }
    }

    private PutItemResult putItem(Map<String, AttributeValue> item) {
        PutItemRequest putItemRequest = new PutItemRequest()
                .withItem(item)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTableName(statusTableName);
        return dynamoDB.putItem(putItemRequest);
    }

    @Override
    public Optional<IngestJobStatus> getJob(String jobId) {
        return getJobStream(jobId).findFirst();
    }

    private Stream<IngestJobStatus> getJobStream(String jobId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(statusTableName)
                .addKeyConditionsEntry(DynamoDBIngestJobStatusFormat.JOB_ID, new Condition()
                        .withAttributeValueList(createStringAttribute(jobId))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(result.getItems().stream());
    }

    @Override
    public List<IngestJobStatus> getJobsByTaskId(TableIdentity tableId, String taskId) {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(
                        streamPagedItems(dynamoDB, createScanRequestByTable(tableId)))
                .filter(job -> job.isTaskIdAssigned(taskId))
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestJobStatus> getUnfinishedJobs(TableIdentity tableId) {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(
                        streamPagedItems(dynamoDB, createScanRequestByTable(tableId)))
                .filter(job -> !job.isFinished())
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestJobStatus> getAllJobs(TableIdentity tableId) {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(
                        streamPagedItems(dynamoDB, createScanRequestByTable(tableId)))
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestJobStatus> getJobsInTimePeriod(TableIdentity tableId, Instant startTime, Instant endTime) {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(
                        streamPagedItems(dynamoDB, createScanRequestByTable(tableId)))
                .filter(job -> job.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestJobStatus> getInvalidJobs() {
        return DynamoDBIngestJobStatusFormat.streamJobStatuses(
                        streamPagedItems(dynamoDB, createScanRequest()))
                .filter(job -> job.getFurthestStatusType().equals(REJECTED))
                .collect(Collectors.toList());
    }

    private ScanRequest createScanRequest() {
        return new ScanRequest().withTableName(statusTableName);
    }

    private ScanRequest createScanRequestByTable(TableIdentity tableId) {
        return createScanRequest()
                .addScanFilterEntry(DynamoDBIngestJobStatusFormat.TABLE_ID, new Condition()
                        .withAttributeValueList(createStringAttribute(tableId.getTableUniqueId()))
                        .withComparisonOperator(ComparisonOperator.EQ));
    }
}
