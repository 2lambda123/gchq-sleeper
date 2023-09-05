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

package sleeper.systemtest.drivers.compaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;

public class CompactionDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionDriver.class);

    private final SleeperInstanceContext instance;
    private final LambdaClient lambdaClient;
    private final AmazonDynamoDB dynamoDBClient;

    public CompactionDriver(SleeperInstanceContext instance, LambdaClient lambdaClient, AmazonDynamoDB dynamoDBClient) {
        this.instance = instance;
        this.lambdaClient = lambdaClient;
        this.dynamoDBClient = dynamoDBClient;
    }

    public List<String> createJobsGetIds() {
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        Set<String> jobsBefore = allJobIds(store).collect(Collectors.toSet());
        InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION));
        List<String> newJobs = allJobIds(store)
                .filter(not(jobsBefore::contains))
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Created {} new compaction jobs", newJobs.size());
        return newJobs;
    }

    public void invokeTasks(Type type, int expectedTasks) throws InterruptedException {
        invokeTasks(type, expectedTasks, PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
    }

    public void invokeTasks(Type type, int expectedTasks, PollWithRetries poll) throws InterruptedException {
        CompactionTaskStatusStore store = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        int tasksFinishedBefore = store.getAllTasks().size() - store.getTasksInProgress().size();
        poll.pollUntil("tasks are started", () -> {
            InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(type.taskCreationProperty));
            int tasksStarted = store.getAllTasks().size() - tasksFinishedBefore;
            LOGGER.info("Found {} running compaction tasks", tasksStarted);
            return tasksStarted >= expectedTasks;
        });
    }

    private Stream<String> allJobIds(CompactionJobStatusStore store) {
        return store.getAllJobs(instance.getTableName()).stream().map(CompactionJobStatus::getJobId);
    }

    public enum Type {
        STANDARD(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION),
        SPLITTING(SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION);

        private final InstanceProperty taskCreationProperty;

        Type(InstanceProperty taskCreationProperty) {
            this.taskCreationProperty = taskCreationProperty;
        }
    }
}
