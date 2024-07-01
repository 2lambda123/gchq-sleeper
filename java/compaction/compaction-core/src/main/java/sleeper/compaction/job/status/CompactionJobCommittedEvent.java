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
package sleeper.compaction.job.status;

import sleeper.compaction.job.CompactionJob;

import java.util.Objects;

/**
 * An event for when a compaction job was committed to the state store. Used in the compaction job status store.
 */
public class CompactionJobCommittedEvent {
    private final String jobId;
    private final String tableId;
    private final String taskId;
    private final String jobRunId;

    private CompactionJobCommittedEvent(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        jobRunId = builder.jobRunId;
    }

    /**
     * Creates a builder for this class.
     *
     * @param  job the compaction job
     * @return     a builder
     */
    public static Builder compactionJobCommitted(CompactionJob job) {
        return builder().job(job);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getJobId() {
        return jobId;
    }

    public String getTableId() {
        return tableId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, taskId, jobRunId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobCommittedEvent)) {
            return false;
        }
        CompactionJobCommittedEvent other = (CompactionJobCommittedEvent) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(tableId, other.tableId) && Objects.equals(taskId, other.taskId) && Objects.equals(jobRunId, other.jobRunId);
    }

    @Override
    public String toString() {
        return "CompactionJobCommittedEvent{jobId=" + jobId + ", tableId=" + tableId + ", taskId=" + taskId + ", jobRunId=" + jobRunId + "}";
    }

    /**
     * Builder for compaction job committed events.
     */
    public static final class Builder {
        private String jobId;
        private String tableId;
        private String jobRunId;
        private String taskId;

        private Builder() {
        }

        /**
         * Sets the compaction job ID and the table ID using the provided compaction job.
         *
         * @param  job the compaction job
         * @return     the builder
         */
        public Builder job(CompactionJob job) {
            return jobId(job.getId())
                    .tableId(job.getTableId());
        }

        /**
         * Sets the compaction job ID.
         *
         * @param  jobId the compaction job ID
         * @return       the builder
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the table ID.
         *
         * @param  tableId the table ID
         * @return         the builder
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the job run ID.
         *
         * @param  jobRunId the job run ID
         * @return          the builder
         */
        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        /**
         * Sets the task ID.
         *
         * @param  taskId the task ID
         * @return        the builder
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public CompactionJobCommittedEvent build() {
            return new CompactionJobCommittedEvent(this);
        }

    }
}
