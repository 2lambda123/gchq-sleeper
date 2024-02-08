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

package sleeper.systemtest.suite.dsl.python;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.ingest.AwsInvokeIngestTasksDriver;
import sleeper.systemtest.drivers.python.PythonIngestDriver;
import sleeper.systemtest.drivers.util.AwsWaitForJobs;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.IngestByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SystemTestPythonIngest {
    private final IngestByAnyQueueDriver byQueueDriver;
    private final InvokeIngestTasksDriver tasksDriver;
    private final WaitForJobs waitForJobs;
    private final List<String> sentJobIds = new ArrayList<>();


    public SystemTestPythonIngest(SleeperInstanceContext instance, SystemTestClients clients,
                                  Path pythonDir) {
        this.byQueueDriver = new PythonIngestDriver(instance, pythonDir);
        this.tasksDriver = new AwsInvokeIngestTasksDriver(instance, clients);
        this.waitForJobs = AwsWaitForJobs.forIngest(instance, clients.getDynamoDB());
    }

    public SystemTestPythonIngest uploadingLocalFile(Path tempDir, String file) {
        String jobId = UUID.randomUUID().toString();
        byQueueDriver.uploadLocalFileAndSendJob(tempDir, jobId, file);
        sentJobIds.add(jobId);
        return this;
    }

    public SystemTestPythonIngest fromS3(String... files) {
        String jobId = UUID.randomUUID().toString();
        byQueueDriver.sendJobWithFiles(jobId, files);
        sentJobIds.add(jobId);
        return this;
    }

    public SystemTestPythonIngest invokeTask() {
        tasksDriver.invokeStandardIngestTask();
        return this;
    }

    public void waitForJobs() throws InterruptedException {
        waitForJobs.waitForJobs(sentJobIds,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(10)));
    }
}
