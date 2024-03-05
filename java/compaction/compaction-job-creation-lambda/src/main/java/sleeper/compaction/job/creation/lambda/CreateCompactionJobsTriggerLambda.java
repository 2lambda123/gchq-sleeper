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
package sleeper.compaction.job.creation.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.core.table.InvokeForTableRequestSerDe;
import sleeper.core.table.TableIndex;
import sleeper.core.util.LoggedDuration;

import java.time.Instant;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_BATCH_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_BATCH_SIZE;

/**
 * Creates batches of tables to create compaction jobs for.
 * Sends these batches to an SQS queue to be picked up by {@link CreateCompactionJobsSQSLambda}.
 */
@SuppressWarnings("unused")
public class CreateCompactionJobsTriggerLambda implements RequestHandler<ScheduledEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateCompactionJobsTriggerLambda.class);

    private final InstanceProperties instanceProperties = new InstanceProperties();
    private final InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private final AmazonSQS sqsClient;
    private final String configBucketName;

    /**
     * No-args constructor used by Lambda.
     */
    public CreateCompactionJobsTriggerLambda() {
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
        this.dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        this.sqsClient = AmazonSQSClientBuilder.defaultClient();
        this.configBucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda triggered at {}, started at {}", event.getTime(), startTime);
        instanceProperties.loadFromS3(s3Client, configBucketName);
        int batchSize = instanceProperties.getInt(COMPACTION_JOB_CREATION_BATCH_SIZE);
        String queueUrl = instanceProperties.get(COMPACTION_JOB_CREATION_BATCH_QUEUE_URL);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        InvokeForTableRequest.forTables(tableIndex.streamOnlineTables(), batchSize,
                request -> sqsClient.sendMessage(queueUrl, serDe.toJson(request)));

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }

}
