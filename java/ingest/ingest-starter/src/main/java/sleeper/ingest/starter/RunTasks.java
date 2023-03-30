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
package sleeper.ingest.starter;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.PropagateTags;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.job.common.CommonJobUtils;
import sleeper.job.common.QueueMessageCount;
import sleeper.job.common.RunECSTasks;

import java.util.ArrayList;
import java.util.List;

/**
 * Finds the number of messages on a queue, and starts up one Fargate task for each, up to a configurable maximum.
 */
public class RunTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunTasks.class);

    private final AmazonSQS sqsClient;
    private final AmazonECS ecsClient;
    private final String sqsJobQueueUrl;
    private final String clusterName;
    private final String containerName;
    private final String taskDefinition;
    private final int maximumRunningTasks;
    private final String subnet;
    private final String bucketName;
    private final String fargateVersion;

    public RunTasks(AmazonSQS sqsClient,
                    AmazonECS ecsClient,
                    String sqsJobQueueUrl,
                    String clusterName,
                    String containerName,
                    String taskDefinition,
                    int maximumRunningTasks,
                    String subnet,
                    String bucketName,
                    String fargateVersion) {
        this.sqsClient = sqsClient;
        this.ecsClient = ecsClient;
        this.sqsJobQueueUrl = sqsJobQueueUrl;
        this.clusterName = clusterName;
        this.containerName = containerName;
        this.taskDefinition = taskDefinition;
        this.maximumRunningTasks = maximumRunningTasks;
        this.subnet = subnet;
        this.bucketName = bucketName;
        this.fargateVersion = fargateVersion;
    }

    public void run() {
        // Find out number of messages in queue that are not being processed
        int queueSize = QueueMessageCount.withSqsClient(sqsClient).getQueueMessageCount(sqsJobQueueUrl)
                .getApproximateNumberOfMessages();
        LOGGER.debug("Queue size is {}", queueSize);
        if (0 == queueSize) {
            LOGGER.info("Finishing as queue size is 0");
            return;
        }

        // Find out number of pending and running tasks
        int numRunningAndPendingTasks = CommonJobUtils.getNumPendingAndRunningTasks(clusterName, ecsClient);
        LOGGER.info("Number of running and pending tasks is {}", numRunningAndPendingTasks);

        // Finish if number of running tasks is already the maximum
        if (numRunningAndPendingTasks == maximumRunningTasks) {
            LOGGER.info("Finishing as number of running tasks is already the maximum");
            return;
        }

        // Calculate maximum number of tasks to create
        int maxNumTasksToCreate = maximumRunningTasks - numRunningAndPendingTasks;
        LOGGER.debug("Maximum number of tasks to create is {}", maxNumTasksToCreate);

        // Create 1 task per ingest jobs up to the maximum number of tasks to create
        int numberOfTasksToCreate = Math.min(queueSize, maxNumTasksToCreate);

        List<String> args = new ArrayList<>();
        args.add(bucketName);

        ContainerOverride containerOverride = new ContainerOverride()
                .withName(containerName)
                .withCommand(args);

        TaskOverride override = new TaskOverride()
                .withContainerOverrides(containerOverride);

        AwsVpcConfiguration vpcConfiguration = new AwsVpcConfiguration()
                .withSubnets(subnet);

        NetworkConfiguration networkConfiguration = new NetworkConfiguration()
                .withAwsvpcConfiguration(vpcConfiguration);

        RunTaskRequest runTaskRequest = new RunTaskRequest()
                .withCluster(clusterName)
                .withLaunchType(LaunchType.FARGATE)
                .withTaskDefinition(taskDefinition)
                .withNetworkConfiguration(networkConfiguration)
                .withOverrides(override)
                .withPropagateTags(PropagateTags.TASK_DEFINITION)
                .withPlatformVersion(fargateVersion);

        RunECSTasks.runTasks(ecsClient, runTaskRequest, numberOfTasksToCreate);
    }
}
