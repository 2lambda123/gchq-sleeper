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
package sleeper.bulkimport.starter;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.Executor;
import sleeper.bulkimport.starter.executor.ExecutorFactory;

import java.io.IOException;

/**
 * The {@link BulkImportStarter} consumes {@link sleeper.bulkimport.job.BulkImportJob} messages from SQS and starts executes them using
 * an {@link Executor}.
 */
public class BulkImportStarter implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportStarter.class);

    private final Executor executor;
    private final BulkImportJobSerDe bulkImportJobSerDe = new BulkImportJobSerDe();

    public BulkImportStarter() throws IOException {
        this(AmazonS3ClientBuilder.defaultClient(), AmazonElasticMapReduceClientBuilder.defaultClient(), AWSStepFunctionsClientBuilder.defaultClient());
    }

    public BulkImportStarter(AmazonS3 s3Client, AmazonElasticMapReduce emrClient, AWSStepFunctions stepFunctionsClient) throws IOException {
        this(new ExecutorFactory(s3Client, emrClient, stepFunctionsClient).createExecutor());
    }

    public BulkImportStarter(Executor executor) {
        this.executor = executor;
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        LOGGER.info("Received request: {}", event);
        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .map(bulkImportJobSerDe::fromJson)
                .forEach(executor::runJob);
        return null;
    }
}
