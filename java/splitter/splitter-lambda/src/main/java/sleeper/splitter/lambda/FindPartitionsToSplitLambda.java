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
package sleeper.splitter.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.core.table.InvokeForTableRequestSerDe;
import sleeper.core.util.LoggedDuration;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.splitter.FindPartitionsToSplit;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * This is triggered via a periodic Cloudwatch rule. It runs
 * {@link FindPartitionsToSplit} for each table.
 */
@SuppressWarnings("unused")
public class FindPartitionsToSplitLambda implements RequestHandler<SQSEvent, Void> {
    private final AmazonSQS sqsClient;
    private final InstanceProperties instanceProperties;
    private final StateStoreProvider stateStoreProvider;
    private final InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();

    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionsToSplitLambda.class);
    private final TablePropertiesProvider tablePropertiesProvider;

    public FindPartitionsToSplitLambda() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        if (null == s3Bucket) {
            throw new RuntimeException("Couldn't get S3 bucket from environment variable");
        }
        this.instanceProperties = new InstanceProperties();
        this.instanceProperties.loadFromS3(s3Client, s3Bucket);
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        this.sqsClient = AmazonSQSClientBuilder.defaultClient();
        this.stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties));
        this.tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda triggered at {}", startTime);
        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .peek(body -> LOGGER.info("Received message: {}", body))
                .map(serDe::fromJson)
                .forEach(this::run);

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }

    private void run(InvokeForTableRequest tableRequest) {
        FindPartitionsToSplit findPartitionsToSplit = new FindPartitionsToSplit(instanceProperties, tablePropertiesProvider, stateStoreProvider,
                new SqsSplitPartitionJobSender(tablePropertiesProvider, instanceProperties, sqsClient)::send);
        tableRequest.getTableIds().stream()
                .map(tablePropertiesProvider::getById)
                .forEach(findPartitionsToSplit::run);
    }
}
