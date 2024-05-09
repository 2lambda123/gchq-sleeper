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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreException;

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;

public class CompactionJobCommitHandler {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobCommitHandler.class);

    private TablePropertiesProvider tablePropertiesProvider;
    private CompactionJobCommitter jobCommitter;
    private CommitQueueSender jobCommitQueueSender;

    public CompactionJobCommitHandler(TablePropertiesProvider tablePropertiesProvider,
            CompactionJobCommitter jobCommitter, CommitQueueSender jobCommitQueueSender) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.jobCommitter = jobCommitter;
        this.jobCommitQueueSender = jobCommitQueueSender;
    }

    public void commit(CompactionJobCommitRequest commitRequest) throws StateStoreException, InterruptedException {
        if (tablePropertiesProvider.getById(commitRequest.getJob().getTableId()).getBoolean(COMPACTION_JOB_COMMIT_ASYNC)) {
            LOGGER.info("Sending compaction job to queue to be commit asynchronously");
            jobCommitQueueSender.send(commitRequest);
        } else {
            LOGGER.info("Committing compaction job syncronously inside compaction task");
            jobCommitter.apply(commitRequest);
        }
    }

    interface CommitQueueSender {
        void send(CompactionJobCommitRequest commitRequest);
    }
}
