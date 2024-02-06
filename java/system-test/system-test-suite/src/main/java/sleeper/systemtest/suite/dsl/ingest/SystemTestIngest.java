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

package sleeper.systemtest.suite.dsl.ingest;

import sleeper.systemtest.drivers.ingest.DirectEmrServerlessDriver;
import sleeper.systemtest.drivers.ingest.DirectIngestDriver;
import sleeper.systemtest.drivers.ingest.IngestBatcherDriver;
import sleeper.systemtest.drivers.ingest.IngestByQueueDriver;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.util.WaitForJobsDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.nio.file.Path;

public class SystemTestIngest {
    private final SystemTestClients clients;
    private final SleeperInstanceContext instance;
    private final IngestSourceFilesDriver sourceFiles;

    public SystemTestIngest(
            SystemTestClients clients,
            SleeperInstanceContext instance,
            IngestSourceFilesDriver sourceFiles) {
        this.clients = clients;
        this.instance = instance;
        this.sourceFiles = sourceFiles;
    }

    public SystemTestIngest setType(SystemTestIngestType type) {
        type.applyTo(instance);
        return this;
    }

    public SystemTestIngestBatcher batcher() {
        return new SystemTestIngestBatcher(this, sourceFiles, instance,
                new IngestBatcherDriver(instance, clients.getDynamoDB(), clients.getSqs(), clients.getLambda()));
    }

    public SystemTestDirectIngest direct(Path tempDir) {
        return new SystemTestDirectIngest(instance, new DirectIngestDriver(instance, tempDir));
    }

    public SystemTestIngestToStateStore toStateStore() {
        return new SystemTestIngestToStateStore(instance);
    }

    public SystemTestIngestByQueue byQueue() {
        return new SystemTestIngestByQueue(sourceFiles, byQueueDriver(), waitForIngestJobsDriver());
    }

    public SystemTestIngestByQueue bulkImportByQueue() {
        return new SystemTestIngestByQueue(sourceFiles, byQueueDriver(), waitForBulkImportJobsDriver());
    }

    IngestByQueueDriver byQueueDriver() {
        return new IngestByQueueDriver(instance, clients.getDynamoDB(), clients.getLambda(), clients.getSqs());
    }

    WaitForJobsDriver waitForIngestJobsDriver() {
        return WaitForJobsDriver.forIngest(instance, clients.getDynamoDB());
    }

    WaitForJobsDriver waitForBulkImportJobsDriver() {
        return WaitForJobsDriver.forBulkImport(instance, clients.getDynamoDB());
    }

    public SystemTestDirectEmrServerless directEmrServerless() {
        return new SystemTestDirectEmrServerless(instance, sourceFiles,
                new DirectEmrServerlessDriver(instance,
                        clients.getS3(), clients.getDynamoDB(), clients.getEmrServerless()),
                waitForBulkImportJobsDriver());
    }
}
