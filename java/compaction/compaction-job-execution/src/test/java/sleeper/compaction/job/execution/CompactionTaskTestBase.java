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
package sleeper.compaction.job.execution;

import org.junit.jupiter.api.BeforeEach;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.execution.CompactionTask.CompactionRunner;
import sleeper.compaction.job.execution.CompactionTask.MessageHandle;
import sleeper.compaction.job.execution.CompactionTask.MessageReceiver;
import sleeper.compaction.job.execution.CompactionTask.WaitForFileAssignments;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.compaction.testutils.InMemoryCompactionTaskStatusStore;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class CompactionTaskTestBase {
    protected static final String DEFAULT_TABLE_ID = "test-table-id";
    protected static final String DEFAULT_TABLE_NAME = "test-table-name";
    protected static final String DEFAULT_TASK_ID = "test-task-id";
    protected static final Instant DEFAULT_CREATED_TIME = Instant.parse("2024-03-04T10:50:00Z");

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final Schema schema = schemaWithKey("key");
    protected final TableProperties tableProperties = createTable(DEFAULT_TABLE_ID, DEFAULT_TABLE_NAME);
    protected final StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);
    protected final FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
    protected final Queue<CompactionJob> jobsOnQueue = new LinkedList<>();
    protected final List<CompactionJob> successfulJobs = new ArrayList<>();
    protected final List<CompactionJob> failedJobs = new ArrayList<>();
    protected final InMemoryCompactionJobStatusStore jobStore = new InMemoryCompactionJobStatusStore();
    protected final CompactionTaskStatusStore taskStore = new InMemoryCompactionTaskStatusStore();
    protected final List<Duration> sleeps = new ArrayList<>();
    protected final List<CompactionJobCommitRequest> commitRequestsOnQueue = new ArrayList<>();

    @BeforeEach
    void setUp() {
        instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 10);
    }

    protected TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        return tableProperties;
    }

    protected void runTask(CompactionRunner compactor) throws Exception {
        runTask(compactor, Instant::now);
    }

    protected void runTask(CompactionRunner compactor, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), filesImmediatelyAssigned(), compactor, timeSupplier, DEFAULT_TASK_ID);
    }

    protected void runTask(CompactionRunner compactor, Supplier<Instant> timeSupplier,
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider) throws Exception {
        runTask(pollQueue(), filesImmediatelyAssigned(), compactor, timeSupplier, DEFAULT_TASK_ID, tablePropertiesProvider, stateStoreProvider);
    }

    protected void runTask(String taskId, CompactionRunner compactor, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), filesImmediatelyAssigned(), compactor, timeSupplier, taskId);
    }

    protected void runTaskCheckingFiles(WaitForFileAssignments waitForFiles, CompactionRunner compactor) throws Exception {
        runTask(pollQueue(), waitForFiles, compactor, Instant::now, DEFAULT_TASK_ID);
    }

    protected void runTask(
            MessageReceiver messageReceiver,
            CompactionRunner compactor,
            Supplier<Instant> timeSupplier) throws Exception {
        runTask(messageReceiver, filesImmediatelyAssigned(), compactor, timeSupplier, DEFAULT_TASK_ID);
    }

    private void runTask(
            MessageReceiver messageReceiver,
            WaitForFileAssignments waitForFiles,
            CompactionRunner compactor,
            Supplier<Instant> timeSupplier,
            String taskId) throws Exception {
        runTask(messageReceiver, waitForFiles, compactor, timeSupplier, taskId,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore));
    }

    private void runTask(
            MessageReceiver messageReceiver,
            WaitForFileAssignments waitForFiles,
            CompactionRunner compactor,
            Supplier<Instant> timeSupplier,
            String taskId,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider) throws Exception {
        CompactionJobCommitterOrSendToLambda committer = new CompactionJobCommitterOrSendToLambda(
                tablePropertiesProvider,
                new CompactionJobCommitter(jobStore, tableId -> stateStoreProvider.getStateStore(tablePropertiesProvider.getById(tableId))),
                commitRequestsOnQueue::add);
        new CompactionTask(instanceProperties,
                PropertiesReloader.neverReload(), messageReceiver, compactor, waitForFiles,
                committer, jobStore, taskStore, taskId, timeSupplier, sleeps::add)
                .run();
    }

    private WaitForFileAssignments filesImmediatelyAssigned() {
        return job -> {
        };
    }

    protected CompactionJob createJobOnQueue(String jobId) throws Exception {
        return createJobOnQueue(jobId, tableProperties, stateStore);
    }

    protected CompactionJob createJobOnQueue(String jobId, TableProperties tableProperties, StateStore stateStore) throws Exception {
        CompactionJob job = createJob(jobId, tableProperties, stateStore);
        jobsOnQueue.add(job);
        jobStore.jobCreated(job, DEFAULT_CREATED_TIME);
        return job;
    }

    protected CompactionJob createJob(String jobId) throws Exception {
        return createJob(jobId, tableProperties, stateStore);
    }

    protected CompactionJob createJob(String jobId, TableProperties tableProperties, StateStore stateStore) throws Exception {
        String inputFile = UUID.randomUUID().toString();
        CompactionJob job = CompactionJob.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .jobId(jobId)
                .partitionId("root")
                .inputFiles(List.of(inputFile))
                .outputFile(UUID.randomUUID().toString()).build();
        assignFilesToJob(job, stateStore);
        return job;
    }

    protected void assignFilesToJob(CompactionJob job, StateStore stateStore) throws Exception {
        for (String inputFile : job.getInputFiles()) {
            stateStore.addFile(factory.rootFile(inputFile, 123L));
        }
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(job.getId(), job.getPartitionId(), job.getInputFiles())));
    }

    protected void send(CompactionJob job) {
        jobsOnQueue.add(job);
    }

    private MessageReceiver pollQueue() {
        return () -> {
            CompactionJob job = jobsOnQueue.poll();
            if (job != null) {
                return Optional.of(new FakeMessageHandle(job));
            } else {
                return Optional.empty();
            }
        };
    }

    protected MessageReceiver pollQueue(MessageReceiver... actions) {
        Iterator<MessageReceiver> getAction = List.of(actions).iterator();
        return () -> {
            if (getAction.hasNext()) {
                return getAction.next().receiveMessage();
            } else {
                throw new IllegalStateException("Unexpected queue poll");
            }
        };
    }

    protected MessageReceiver receiveJob() {
        return () -> {
            if (jobsOnQueue.isEmpty()) {
                throw new IllegalStateException("Expected job on queue");
            }
            return Optional.of(new FakeMessageHandle(jobsOnQueue.poll()));
        };
    }

    protected MessageReceiver receiveNoJob() {
        return () -> {
            if (!jobsOnQueue.isEmpty()) {
                throw new IllegalStateException("Expected no jobs on queue");
            }
            return Optional.empty();
        };
    }

    protected MessageReceiver receiveNoJobAnd(Runnable action) {
        return () -> {
            if (!jobsOnQueue.isEmpty()) {
                throw new IllegalStateException("Expected no jobs on queue");
            }
            action.run();
            return Optional.empty();
        };
    }

    protected CompactionRunner jobsSucceed(int numJobs) {
        return processJobs(Stream.generate(() -> jobSucceeds())
                .limit(numJobs)
                .toArray(ProcessJob[]::new));
    }

    protected ProcessJob jobSucceeds(RecordsProcessed summary) {
        return new ProcessJob(summary);
    }

    protected ProcessJob jobSucceeds() {
        return new ProcessJob(10L);
    }

    protected ProcessJob jobFails() {
        return new ProcessJob(new RuntimeException("Something failed"));
    }

    protected ProcessJob jobFails(RuntimeException failure) {
        return new ProcessJob(failure);
    }

    protected CompactionRunner processNoJobs() {
        return processJobs();
    }

    protected CompactionRunner processJobs(ProcessJob... actions) {
        Iterator<ProcessJob> getAction = List.of(actions).iterator();
        return job -> {
            if (getAction.hasNext()) {
                ProcessJob action = getAction.next();
                if (action.failure != null) {
                    throw action.failure;
                } else {
                    successfulJobs.add(job);
                    return action.recordsProcessed;
                }
            } else {
                throw new IllegalStateException("Unexpected job: " + job);
            }
        };
    }

    protected class ProcessJob {
        private final RuntimeException failure;
        private final RecordsProcessed recordsProcessed;

        ProcessJob(RuntimeException failure) {
            this.failure = failure;
            this.recordsProcessed = null;
        }

        ProcessJob(long records) {
            this(new RecordsProcessed(records, records));
        }

        ProcessJob(RecordsProcessed summary) {
            this.failure = null;
            this.recordsProcessed = summary;
        }
    }

    protected class FakeMessageHandle implements MessageHandle {
        private final CompactionJob job;

        FakeMessageHandle(CompactionJob job) {
            this.job = job;
        }

        public CompactionJob getJob() {
            return job;
        }

        public void close() {
        }

        public void completed() {
        }

        public void failed() {
            failedJobs.add(job);
        }
    }
}
