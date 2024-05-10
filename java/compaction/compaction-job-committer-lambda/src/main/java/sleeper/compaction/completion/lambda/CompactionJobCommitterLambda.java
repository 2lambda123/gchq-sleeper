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
package sleeper.compaction.completion.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitRequestSerDe;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.commit.CompactionJobCommitter.GetStateStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.LoggedDuration;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreProviderWithSize;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class CompactionJobCommitterLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobCommitterLambda.class);

    private final CompactionJobCommitter compactionJobCommitter;
    private final CompactionJobCommitRequestSerDe serDe = new CompactionJobCommitRequestSerDe();

    public CompactionJobCommitterLambda() {
        this(connectToAws());
    }

    public CompactionJobCommitterLambda(CompactionJobCommitter compactionJobCommitter) {
        this.compactionJobCommitter = compactionJobCommitter;
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda started at {}", startTime);
        List<BatchItemFailure> batchItemFailures = new ArrayList<>();
        for (SQSMessage message : event.getRecords()) {
            try {
                LOGGER.info("Found message: {}", message.getBody());
                CompactionJobCommitRequest request = serDe.fromJson(message.getBody());
                compactionJobCommitter.apply(request);
                LOGGER.info("Successfully committed compaction job {}", request.getJob());
            } catch (RuntimeException | StateStoreException | InterruptedException e) {
                LOGGER.error("Failed committing compaction job", e);
                batchItemFailures.add(new BatchItemFailure(message.getMessageId()));
            }
        }
        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})",
                finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return new SQSBatchResponse(batchItemFailures);
    }

    private static CompactionJobCommitter connectToAws() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);
        Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        StateStoreProviderWithSize stateStoreProvider = new StateStoreProviderWithSize(instanceProperties, s3Client, dynamoDBClient, hadoopConf);
        CompactionJobStatusStore statusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
        return new CompactionJobCommitter(
                statusStore, stateStoreProviderForCommitter(tablePropertiesProvider, stateStoreProvider));
    }

    private static GetStateStore stateStoreProviderForCommitter(
            TablePropertiesProvider tablePropertiesProvider, StateStoreProviderWithSize stateStoreProvider) {
        return tableId -> stateStoreProvider.getStateStore(tablePropertiesProvider.getById(tableId));
    }

}
