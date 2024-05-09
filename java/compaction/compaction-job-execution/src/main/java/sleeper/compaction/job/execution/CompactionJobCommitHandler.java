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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitRequestSerDe;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreException;

import java.util.UUID;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;

public class CompactionJobCommitHandler {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobCommitHandler.class);

    private TablePropertiesProvider tablePropertiesProvider;
    private CompactionJobCommitter jobCommitter;
    private CommitQueueSender jobCommitQueueSender;

    public CompactionJobCommitHandler(TablePropertiesProvider tablePropertiesProvider,
            CompactionJobCommitter jobCommitter, InstanceProperties instanceProperties, AmazonSQS sqsClient) {
        this(tablePropertiesProvider, jobCommitter, sendToSqs(instanceProperties, sqsClient));
    }

    protected CompactionJobCommitHandler(TablePropertiesProvider tablePropertiesProvider,
            CompactionJobCommitter jobCommitter, CommitQueueSender jobCommitQueueSender) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.jobCommitter = jobCommitter;
        this.jobCommitQueueSender = jobCommitQueueSender;
    }

    public void commit(CompactionJobCommitRequest commitRequest) throws StateStoreException, InterruptedException {
        if (tablePropertiesProvider.getById(commitRequest.getJob().getTableId()).getBoolean(COMPACTION_JOB_COMMIT_ASYNC)) {
            LOGGER.info("Sending compaction job {} to queue to be committed asynchronously", commitRequest.getJob().getId());
            jobCommitQueueSender.send(commitRequest);
        } else {
            LOGGER.info("Committing compaction job {} inside compaction task", commitRequest.getJob().getId());
            jobCommitter.apply(commitRequest);
        }
    }

    interface CommitQueueSender {
        void send(CompactionJobCommitRequest commitRequest);
    }

    public static CommitQueueSender sendToSqs(InstanceProperties instanceProperties, AmazonSQS sqsClient) {
        return request -> {
            String queueUrl = instanceProperties.get(COMPACTION_JOB_COMMITTER_QUEUE_URL);
            String tableId = request.getJob().getTableId();
            sqsClient.sendMessageBatch(new SendMessageBatchRequest()
                    .withQueueUrl(queueUrl)
                    .withEntries(new SendMessageBatchRequestEntry()
                            .withMessageDeduplicationId(UUID.randomUUID().toString())
                            .withId(request.getJob().getId())
                            .withMessageGroupId(tableId)
                            .withMessageBody(new CompactionJobCommitRequestSerDe().toJson(request))));
        };
    }
}
