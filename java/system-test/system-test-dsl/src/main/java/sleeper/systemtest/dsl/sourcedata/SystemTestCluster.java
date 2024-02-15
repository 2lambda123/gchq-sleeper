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

package sleeper.systemtest.dsl.sourcedata;

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.ingest.IngestByQueue;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SystemTestCluster {

    private final DeployedSystemTestResources context;
    private final DataGenerationTasksDriver driver;
    private final IngestByQueue ingestByQueue;
    private final GeneratedIngestSourceFilesDriver sourceFiles;
    private final InvokeIngestTasksDriver tasksDriver;
    private final WaitForJobs waitForIngestJobs;
    private final WaitForJobs waitForBulkImportJobs;
    private GeneratedIngestSourceFiles lastGeneratedFiles = null;
    private final List<String> jobIds = new ArrayList<>();

    public SystemTestCluster(SystemTestContext context, SystemTestDrivers drivers) {
        this.context = context.systemTest();
        driver = drivers.dataGenerationTasks(context);
        ingestByQueue = drivers.ingestByQueue(context);
        sourceFiles = drivers.generatedSourceFiles(context.parameters(), context.systemTest());
        tasksDriver = drivers.invokeIngestTasks(context);
        waitForIngestJobs = drivers.waitForIngest(context);
        waitForBulkImportJobs = drivers.waitForBulkImport(context);
    }

    public SystemTestCluster updateProperties(Consumer<SystemTestStandaloneProperties> config) {
        context.updateProperties(config);
        return this;
    }

    public SystemTestCluster generateData() {
        return generateData(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(2)));
    }

    public SystemTestCluster generateData(PollWithRetries poll) {
        driver.runDataGenerationTasks(poll);
        lastGeneratedFiles = sourceFiles.findGeneratedFiles();
        return this;
    }

    public SystemTestCluster sendAllGeneratedFilesAsOneJob(InstanceProperty queueUrlProperty) {
        jobIds.add(ingestByQueue.sendJobGetId(queueUrlProperty, lastGeneratedFiles.getIngestJobFilesCombiningAll()));
        return this;
    }

    public SystemTestCluster invokeStandardIngestTask() {
        tasksDriver.invokeStandardIngestTask();
        return this;
    }

    public SystemTestCluster invokeStandardIngestTasks(int expectedTasks, PollWithRetries poll) {
        tasksDriver.invokeStandardIngestTasks(expectedTasks, poll);
        return this;
    }

    public void waitForIngestJobs() {
        waitForIngestJobs.waitForJobs(jobIds());
    }

    public void waitForIngestJobs(PollWithRetries poll) {
        waitForIngestJobs.waitForJobs(jobIds(), poll);
    }

    public void waitForBulkImportJobs(PollWithRetries poll) {
        waitForBulkImportJobs.waitForJobs(jobIds(), poll);
    }

    private List<String> jobIds() {
        if (jobIds.isEmpty()) {
            jobIds.addAll(lastGeneratedFiles.getJobIdsFromIndividualFiles());
        }
        return jobIds;
    }

    public List<String> findIngestJobIdsInSourceBucket() {
        return sourceFiles.findGeneratedFiles().getJobIdsFromIndividualFiles();
    }

    public boolean isDisabled() {
        return !context.isSystemTestClusterEnabled();
    }
}
