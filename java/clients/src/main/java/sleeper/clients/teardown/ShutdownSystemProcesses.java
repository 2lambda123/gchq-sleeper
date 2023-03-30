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
package sleeper.clients.teardown;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.ListTasksRequest;
import com.amazonaws.services.ecs.model.ListTasksResult;
import com.amazonaws.services.ecs.model.StopTaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.status.update.PauseSystem;

import java.util.List;
import java.util.function.Consumer;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;
import static sleeper.core.util.RateLimitUtils.sleepForSustainedRatePerSecond;

public class ShutdownSystemProcesses {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownSystemProcesses.class);

    private final AmazonCloudWatchEvents cloudWatch;
    private final AmazonECS ecs;

    public ShutdownSystemProcesses(AmazonCloudWatchEvents cloudWatch, AmazonECS ecs) {
        this.cloudWatch = cloudWatch;
        this.ecs = ecs;
    }

    public void shutdown(
            InstanceProperties instanceProperties,
            List<InstanceProperty> extraECSClusters) {
        LOGGER.info("Pausing the system");
        PauseSystem.pause(cloudWatch, instanceProperties);
        stopECSTasks(instanceProperties, extraECSClusters);
    }

    private void stopECSTasks(InstanceProperties instanceProperties, List<InstanceProperty> extraClusters) {
        stopTasks(ecs, instanceProperties, INGEST_CLUSTER);
        stopTasks(ecs, instanceProperties, COMPACTION_CLUSTER);
        stopTasks(ecs, instanceProperties, SPLITTING_COMPACTION_CLUSTER);
        extraClusters.forEach(clusterName -> stopTasks(ecs, instanceProperties, clusterName));
    }

    private static void stopTasks(AmazonECS ecs, InstanceProperties properties, InstanceProperty property) {
        if (!properties.isSet(property)) {
            return;
        }
        String clusterName = properties.get(property);
        LOGGER.info("Stopping tasks for ECS cluster {}", clusterName);
        forEachTaskArn(ecs, clusterName, taskArn -> {
            // Rate limit for ECS StopTask is 100 burst, 40 sustained:
            // https://docs.aws.amazon.com/AmazonECS/latest/APIReference/request-throttling.html
            sleepForSustainedRatePerSecond(30);
            ecs.stopTask(new StopTaskRequest()
                    .withCluster(clusterName)
                    .withTask(taskArn)
                    .withReason("Cleaning up before cdk destroy"));
        });
    }

    private static void forEachTaskArn(AmazonECS ecs, String clusterName, Consumer<String> consumer) {
        String nextToken = null;
        do {
            ListTasksResult result = ecs.listTasks(new ListTasksRequest()
                    .withCluster(clusterName).withNextToken(nextToken));

            LOGGER.info("Found {} tasks", result.getTaskArns().size());
            result.getTaskArns().forEach(consumer);
            nextToken = result.getNextToken();
        } while (nextToken != null);
    }
}
