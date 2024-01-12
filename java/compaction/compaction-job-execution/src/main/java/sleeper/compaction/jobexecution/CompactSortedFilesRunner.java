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
package sleeper.compaction.jobexecution;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.task.CompactionTaskType;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.LoggedDuration;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.job.common.CommonJobUtils;
import sleeper.job.common.action.ActionException;
import sleeper.job.common.action.ChangeMessageVisibilityTimeoutAction;
import sleeper.job.common.action.DeleteMessageAction;
import sleeper.job.common.action.MessageReference;
import sleeper.job.common.action.thread.PeriodicActionRunnable;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;

/**
 * Retrieves compaction {@link CompactionJob}s from an SQS queue, and executes
 * them. It delegates the actual execution of the job to an instance of
 * {@link CompactSortedFiles}. It passes a
 * {@link ChangeMessageVisibilityTimeoutAction} to
 * that class so that the message on the SQS queue can be kept alive whilst the job
 * is executing. It also handles deletion of the message when the job is completed.
 */
public class CompactSortedFilesRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactSortedFilesRunner.class);
    private static final int DEFAULT_MAX_RETRIEVE_ATTEMPTS = 3;
    private static final int DEFAULT_WAIT_TIME = 20;
    private static final int DEFAULT_FILES_NOT_ASSIGNED_RETRY_DELAY_SECONDS = 5;

    private final InstanceProperties instanceProperties;
    private final ObjectFactory objectFactory;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final PropertiesReloader propertiesReloader;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore jobStatusStore;
    private final CompactionTaskStatusStore taskStatusStore;
    private final String taskId;
    private final String sqsJobQueueUrl;
    private final AmazonSQS sqsClient;
    private final QueueMessageVisibility.Client queueMessageVisibilityClient;
    private final AmazonECS ecsClient;
    private final CompactionTaskType type;
    private final int keepAliveFrequency;
    private final int maxMessageRetrieveAttempts;
    private final int waitTimeSeconds;
    private final int filesNotAssignedRetryDelaySeconds;

    private CompactSortedFilesRunner(Builder builder) {
        instanceProperties = builder.instanceProperties;
        objectFactory = builder.objectFactory;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        propertiesReloader = builder.propertiesReloader;
        stateStoreProvider = builder.stateStoreProvider;
        jobStatusStore = builder.jobStatusStore;
        taskStatusStore = builder.taskStatusStore;
        taskId = builder.taskId;
        sqsJobQueueUrl = builder.sqsJobQueueUrl;
        sqsClient = builder.sqsClient;
        queueMessageVisibilityClient = builder.queueMessageVisibilityClient;
        ecsClient = builder.ecsClient;
        type = builder.type;
        keepAliveFrequency = builder.keepAliveFrequency;
        maxMessageRetrieveAttempts = builder.maxMessageRetrieveAttempts;
        waitTimeSeconds = builder.waitTimeSeconds;
        filesNotAssignedRetryDelaySeconds = builder.filesNotAssignedRetryDelaySeconds;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void run() throws InterruptedException, IOException {
        Instant startTime = Instant.now();
        CompactionTaskStatus.Builder taskStatusBuilder = CompactionTaskStatus
                .builder().taskId(taskId).type(type).startTime(startTime);
        LOGGER.info("Starting task {}", taskId);

        // Log some basic data if running on EC2 inside ECS
        if (instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE).equalsIgnoreCase("EC2")) {
            try {
                if (this.ecsClient != null) {
                    CommonJobUtils.retrieveContainerMetadata(ecsClient).ifPresent(info ->
                            LOGGER.info("Task running on EC2 instance ID {} in AZ {} with ARN {} in cluster {} with status {}",
                                    info.instanceID, info.az, info.instanceARN, info.clusterName, info.status));
                } else {
                    LOGGER.warn("ECS client is null");
                }
            } catch (IOException e) {
                LOGGER.warn("EC2 instance data not available", e);
            }
        }

        taskStatusStore.taskStarted(taskStatusBuilder.build());
        CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
        long totalNumberOfMessagesProcessed = 0L;
        int numConsecutiveTimesNoMessages = 0;
        while (numConsecutiveTimesNoMessages < maxMessageRetrieveAttempts) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsJobQueueUrl)
                    .withMaxNumberOfMessages(1)
                    .withWaitTimeSeconds(waitTimeSeconds); // Must be >= 0 and <= 20
            ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
            if (receiveMessageResult.getMessages().isEmpty()) {
                LOGGER.info("Received no messages in {} seconds", waitTimeSeconds);
                numConsecutiveTimesNoMessages++;
                Thread.sleep(10000L);
            } else {
                Message message = receiveMessageResult.getMessages().get(0);
                LOGGER.info("Received message: {}", message);
                CompactionJob compactionJob = CompactionJobSerDe.deserialiseFromString(message.getBody());
                LOGGER.info("CompactionJob is: {}", compactionJob);
                try {
                    taskFinishedBuilder.addJobSummary(compact(compactionJob, message));
                    totalNumberOfMessagesProcessed++;
                    numConsecutiveTimesNoMessages = 0;
                } catch (FilesNotAssignedException e) {
                    LOGGER.error("Failed processing compaction job, putting job back on queue. Will retry after {} seconds",
                            filesNotAssignedRetryDelaySeconds, e);
                    numConsecutiveTimesNoMessages++;
                    queueMessageVisibilityClient.update(sqsJobQueueUrl, message.getReceiptHandle(), filesNotAssignedRetryDelaySeconds);
                } catch (Exception e) {
                    LOGGER.error("Failed processing compaction job, putting job back on queue. Will retry immediately", e);
                    numConsecutiveTimesNoMessages++;
                    queueMessageVisibilityClient.update(sqsJobQueueUrl, message.getReceiptHandle(), 0);
                }
            }
        }
        LOGGER.info("Returning from run() method in CompactSortedFilesRunner as no messages received in {} seconds",
                (numConsecutiveTimesNoMessages * waitTimeSeconds));
        LOGGER.info("Total number of messages processed = {}", totalNumberOfMessagesProcessed);

        Instant finishTime = Instant.now();
        LOGGER.info("CompactSortedFilesRunner total run time = {}", LoggedDuration.withFullOutput(startTime, finishTime));

        CompactionTaskStatus taskFinished = taskStatusBuilder.finished(finishTime, taskFinishedBuilder).build();
        taskStatusStore.taskFinished(taskFinished);
    }

    private RecordsProcessedSummary compact(CompactionJob compactionJob, Message message)
            throws IOException, IteratorException, ActionException, StateStoreException {
        MessageReference messageReference = new MessageReference(sqsClient, sqsJobQueueUrl,
                "Compaction job " + compactionJob.getId(), message.getReceiptHandle());
        // Create background thread to keep messages alive
        PeriodicActionRunnable keepAliveRunnable = new PeriodicActionRunnable(
                messageReference.changeVisibilityTimeoutAction(
                        instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)),
                keepAliveFrequency);
        keepAliveRunnable.start();
        LOGGER.info("Compaction job {}: Created background thread to keep SQS messages alive (period is {} seconds)",
                compactionJob.getId(), keepAliveFrequency);

        RecordsProcessedSummary summary;
        try {
            summary = compact(compactionJob);
        } finally {
            LOGGER.info("Compaction job {}: Stopping background thread to keep SQS messages alive",
                    compactionJob.getId());
            keepAliveRunnable.stop();
        }

        // Delete message from queue
        LOGGER.info("Compaction job {}: Deleting message from queue", compactionJob.getId());
        DeleteMessageAction deleteAction = messageReference.deleteAction();
        deleteAction.call();

        return summary;
    }

    private RecordsProcessedSummary compact(CompactionJob compactionJob) throws IteratorException, IOException, StateStoreException {
        propertiesReloader.reloadIfNeeded();
        TableProperties tableProperties = tablePropertiesProvider.getById(compactionJob.getTableId());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(instanceProperties, tableProperties, objectFactory,
                compactionJob, stateStore, jobStatusStore, taskId);
        return compactSortedFiles.compact();
    }

    public static void main(String[] args)
            throws InterruptedException, IOException, ObjectFactoryException {
        if (2 != args.length) {
            System.err.println("Error: must have 2 arguments (config bucket and compaction type (compaction or splittingcompaction)), got "
                    + args.length
                    + " arguments (" + StringUtils.join(args, ',') + ")");
            System.exit(1);
        }

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();

        String s3Bucket = args[0];
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        PropertiesReloader propertiesReloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties,
                HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        CompactionJobStatusStore jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient,
                instanceProperties);
        CompactionTaskStatusStore taskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient,
                instanceProperties);

        String sqsJobQueueUrl;
        String typeStr = args[1];
        CompactionTaskType type;
        if (typeStr.equals("compaction")) {
            type = CompactionTaskType.COMPACTION;
            sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
        } else if (typeStr.equals("splittingcompaction")) {
            type = CompactionTaskType.SPLITTING;
            sqsJobQueueUrl = instanceProperties.get(SPLITTING_COMPACTION_JOB_QUEUE_URL);
        } else {
            throw new RuntimeException("Invalid type: got " + typeStr + ", should be 'compaction' or 'splittingcompaction'");
        }

        ObjectFactory objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");
        CompactSortedFilesRunner runner = CompactSortedFilesRunner.builder()
                .instanceProperties(instanceProperties)
                .objectFactory(objectFactory)
                .tablePropertiesProvider(tablePropertiesProvider)
                .propertiesReloader(propertiesReloader)
                .stateStoreProvider(stateStoreProvider)
                .jobStatusStore(jobStatusStore)
                .taskStatusStore(taskStatusStore)
                .taskId(UUID.randomUUID().toString())
                .sqsJobQueueUrl(sqsJobQueueUrl)
                .type(type)
                .buildWithDefaultClients();
        runner.run();

        sqsClient.shutdown();
        LOGGER.info("Shut down sqsClient");
        dynamoDBClient.shutdown();
        LOGGER.info("Shut down dynamoDBClient");
        s3Client.shutdown();
        LOGGER.info("Shut down s3Client");
        ecsClient.shutdown();
        LOGGER.info("Shut down ecsClient");
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private ObjectFactory objectFactory;
        private TablePropertiesProvider tablePropertiesProvider;
        private PropertiesReloader propertiesReloader;
        private StateStoreProvider stateStoreProvider;
        private CompactionJobStatusStore jobStatusStore;
        private CompactionTaskStatusStore taskStatusStore;
        private String taskId;
        private String sqsJobQueueUrl;
        private AmazonSQS sqsClient;
        private QueueMessageVisibility.Client queueMessageVisibilityClient;
        private AmazonECS ecsClient;
        private CompactionTaskType type;
        private int keepAliveFrequency;
        private int maxMessageRetrieveAttempts = DEFAULT_MAX_RETRIEVE_ATTEMPTS;
        private int waitTimeSeconds = DEFAULT_WAIT_TIME;
        private int filesNotAssignedRetryDelaySeconds = DEFAULT_FILES_NOT_ASSIGNED_RETRY_DELAY_SECONDS;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            this.keepAliveFrequency = instanceProperties.getInt(COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS);
            return this;
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public Builder propertiesReloader(PropertiesReloader propertiesReloader) {
            this.propertiesReloader = propertiesReloader;
            return this;
        }

        public Builder stateStoreProvider(StateStoreProvider stateStoreProvider) {
            this.stateStoreProvider = stateStoreProvider;
            return this;
        }

        public Builder jobStatusStore(CompactionJobStatusStore jobStatusStore) {
            this.jobStatusStore = jobStatusStore;
            return this;
        }

        public Builder taskStatusStore(CompactionTaskStatusStore taskStatusStore) {
            this.taskStatusStore = taskStatusStore;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder sqsJobQueueUrl(String sqsJobQueueUrl) {
            this.sqsJobQueueUrl = sqsJobQueueUrl;
            return this;
        }

        public Builder sqsClient(AmazonSQS sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder queueMessageVisibilityClient(QueueMessageVisibility.Client queueMessageVisibilityClient) {
            this.queueMessageVisibilityClient = queueMessageVisibilityClient;
            return this;
        }

        public Builder ecsClient(AmazonECS ecsClient) {
            this.ecsClient = ecsClient;
            return this;
        }

        public Builder type(CompactionTaskType type) {
            this.type = type;
            return this;
        }

        public Builder maxMessageRetrieveAttempts(int maxMessageRetrieveAttempts) {
            this.maxMessageRetrieveAttempts = maxMessageRetrieveAttempts;
            return this;
        }

        public Builder waitTimeSeconds(int waitTimeSeconds) {
            this.waitTimeSeconds = waitTimeSeconds;
            return this;
        }

        public Builder filesNotAssignedRetryDelaySeconds(int filesNotAssignedRetryDelaySeconds) {
            this.filesNotAssignedRetryDelaySeconds = filesNotAssignedRetryDelaySeconds;
            return this;
        }

        public CompactSortedFilesRunner buildWithDefaultClients() {
            return sqsClient(AmazonSQSClientBuilder.defaultClient())
                    .queueMessageVisibilityClient(QueueMessageVisibility.withSqsClient(sqsClient))
                    .ecsClient(AmazonECSClientBuilder.defaultClient())
                    .build();
        }

        public CompactSortedFilesRunner build() {
            return new CompactSortedFilesRunner(this);
        }
    }
}
