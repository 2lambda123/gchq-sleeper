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
package sleeper.systemtest.compaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.clients.util.PollWithRetries;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.systemtest.util.InvokeSystemTestLambda;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.compaction.job.CompactionJobStatusTestData.defaultUpdateTime;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.job.common.QueueMessageCountsInMemory.visibleMessages;
import static sleeper.job.common.QueueMessageCountsSequence.inOrder;

class WaitForCurrentSplitAddingMissingJobsTest {
    private static final String COMPACTION_JOB_QUEUE_URL = "test-splitting-compaction-job-queue";
    private final InstanceProperties properties = createTestInstanceProperties();
    private final String tableName = "test-table";
    private final String taskId = "test-task";
    private final CompactionJobTestDataHelper jobHelper = CompactionJobTestDataHelper.forTable(tableName);
    private final CompactionJobStatusStoreInMemory statusStore = new CompactionJobStatusStoreInMemory();

    @BeforeEach
    void setUp() {
        properties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, COMPACTION_JOB_QUEUE_URL);
    }

    @Test
    void shouldRunOneRoundOfSplitsWithOneSplittingCompactionJob() throws Exception {
        // Given
        CompactionJob job = jobHelper.singleFileCompaction();
        Instant createTime = Instant.parse("2023-05-15T12:11:00Z");
        RecordsProcessedSummary summary = summary(
                Instant.parse("2023-05-15T12:12:00Z"), Duration.ofMinutes(1), 100L, 100L);

        WaitForCurrentSplitAddingMissingJobs waiter = runningOneJob(job, createTime, summary)
                .queueClient(inOrder(visibleMessages(COMPACTION_JOB_QUEUE_URL, 1)))
                .waitForCompactionsToAppearOnQueue(pollTimes(1))
                .build();

        // When/Then
        assertThat(waiter.checkIfSplittingCompactionNeededAndWait()).isTrue();
        assertThat(statusStore.getAllJobs(tableName)).containsExactly(
                jobCreated(job, createTime, finishedCompactionRun(taskId, summary)));
    }

    @Test
    void shouldWaitForCompactionJobToAppearOnQueue() throws Exception {
        // Given
        WaitForCurrentSplitAddingMissingJobs waiter = runningOneJob()
                .queueClient(inOrder(
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 0),
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 1)))
                .waitForCompactionsToAppearOnQueue(pollTimes(2))
                .build();

        // When/Then
        assertThat(waiter.checkIfSplittingCompactionNeededAndWait()).isTrue();
    }

    @Test
    void shouldTimeOutIfCompactionJobDoesNotAppearOnQueue() {
        // Given
        WaitForCurrentSplitAddingMissingJobs waiter = runningOneJob()
                .queueClient(inOrder(
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 0),
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 0)))
                .waitForCompactionsToAppearOnQueue(pollTimes(2))
                .build();

        // When/Then
        assertThatThrownBy(waiter::checkIfSplittingCompactionNeededAndWait)
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }

    // TODO: case where it times out running compaction, polling status store waiting for job to finish
    // TODO: case where job is started immediately after it's created, and queue estimate is zero when first polled
    // TODO: case where queue estimate is zero when first polled, but job is started immediately after that, and estimate stays zero

    private WaitForCurrentSplitAddingMissingJobs.Builder runningOneJob() {
        CompactionJob job = jobHelper.singleFileCompaction();
        Instant createdTime = Instant.parse("2023-05-15T12:11:00Z");
        RecordsProcessedSummary summary = summary(
                Instant.parse("2023-05-15T12:12:00Z"), Duration.ofMinutes(1), 100L, 100L);
        return runningOneJob(job, createdTime, summary);
    }

    private WaitForCurrentSplitAddingMissingJobs.Builder runningOneJob(
            CompactionJob job, Instant createdTime, RecordsProcessedSummary summary) {
        Runnable invokeCompactionJobLambda = () -> {
            statusStore.fixTime(createdTime);
            statusStore.jobCreated(job);
        };
        Runnable invokeCompactionTaskLambda = () -> {
            statusStore.fixTime(defaultUpdateTime(summary.getStartTime()));
            statusStore.jobStarted(job, summary.getStartTime(), taskId);
            statusStore.fixTime(defaultUpdateTime(summary.getFinishTime()));
            statusStore.jobFinished(job, summary, taskId);
        };
        return builder()
                .lambdaClient(invokeCompactionJobAndTaskClient(invokeCompactionJobLambda, invokeCompactionTaskLambda));
    }

    private WaitForCurrentSplitAddingMissingJobs.Builder builder() {
        return WaitForCurrentSplitAddingMissingJobs.builder()
                .instanceProperties(properties)
                .store(statusStore)
                .tableName(tableName)
                .waitForSplitsToFinish(pollTimes(0))
                .waitForCompactionsToAppearOnQueue(pollTimes(0));
    }

    private static InvokeSystemTestLambda.Client invokeCompactionJobAndTaskClient(
            Runnable invokeCompactionJobLambda, Runnable invokeCompactionTaskLambda) {
        return lambdaFunctionProperty -> {
            if (lambdaFunctionProperty.equals(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION)) {
                invokeCompactionJobLambda.run();
            } else if (lambdaFunctionProperty.equals(SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION)) {
                invokeCompactionTaskLambda.run();
            }
        };
    }

    private static PollWithRetries pollTimes(int polls) {
        return PollWithRetries.intervalAndMaxPolls(0, polls);
    }
}
