/*
 * Copyright 2022 Crown Copyright
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
package sleeper.ingest.job;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.ContainerConstants;

import java.io.IOException;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_TASK_DEFINITION_FAMILY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONCURRENT_INGEST_TASKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;

/**
 * A lambda function to execute {@link RunTasks}.
 */
@SuppressWarnings("unused")
public class RunTasksLambda {
    private final RunTasks runTasks;

    public RunTasksLambda() throws IOException {
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        String s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        String containerName = ContainerConstants.INGEST_CONTAINER_NAME;
        String taskDefinition = instanceProperties.get(INGEST_TASK_DEFINITION_FAMILY);

        this.runTasks = new RunTasks(sqsClient,
                ecsClient,
                instanceProperties.get(INGEST_JOB_QUEUE_URL),
                instanceProperties.get(INGEST_CLUSTER),
                containerName,
                taskDefinition,
                instanceProperties.getInt(MAXIMUM_CONCURRENT_INGEST_TASKS),
                instanceProperties.get(SUBNET),
                s3Bucket,
                instanceProperties.get(FARGATE_VERSION));
    }

    public void eventHandler(ScheduledEvent event, Context context) throws InterruptedException {
        runTasks.run();
    }

    private String validateParameter(String parameterName) {
        String parameter = System.getenv(parameterName);
        if (null == parameter || "".equals(parameter)) {
            throw new IllegalArgumentException("RunTasksLambda can't get parameter " + parameterName);
        }
        return parameter;
    }
}
