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

package sleeper.ingest.job.status;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.job.IngestJob;

import java.util.Objects;

/**
 * An event for when an ingest job was finished. Used in the ingest job status store.
 */
public class IngestJobFinishedEvent {
    private final String jobId;
    private final String tableId;
    private final RecordsProcessedSummary summary;
    private final String jobRunId;
    private final String taskId;

    private IngestJobFinishedEvent(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        summary = Objects.requireNonNull(builder.summary, "summary must not be null");
        jobRunId = builder.jobRunId;
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates an instance of this class.
     *
     * @param  taskId  the task ID
     * @param  job     the ingest job
     * @param  summary the records processed summary
     * @return         an instance of this class
     */
    public static IngestJobFinishedEvent ingestJobFinished(String taskId, IngestJob job, RecordsProcessedSummary summary) {
        return ingestJobFinished(job, summary).taskId(taskId).build();
    }

    /**
     * Creates an instance of this class.
     *
     * @param  job     the ingest job
     * @param  summary the records processed summary
     * @return         an instance of this class
     */
    public static Builder ingestJobFinished(IngestJob job, RecordsProcessedSummary summary) {
        return builder().job(job).summary(summary);
    }

    public String getJobId() {
        return jobId;
    }

    public String getTableId() {
        return tableId;
    }

    public RecordsProcessedSummary getSummary() {
        return summary;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public String getTaskId() {
        return taskId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        IngestJobFinishedEvent that = (IngestJobFinishedEvent) object;
        return Objects.equals(jobId, that.jobId)
                && Objects.equals(tableId, that.tableId) && Objects.equals(summary, that.summary)
                && Objects.equals(jobRunId, that.jobRunId) && Objects.equals(taskId, that.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, summary, jobRunId, taskId);
    }

    @Override
    public String toString() {
        return "IngestJobFinishedEvent{" +
                "jobId='" + jobId + '\'' +
                ", tableId='" + tableId + '\'' +
                ", summary=" + summary +
                ", jobRunId='" + jobRunId + '\'' +
                ", taskId='" + taskId + '\'' +
                '}';
    }

    /**
     * Builder for ingest job finished event objects.
     */
    public static final class Builder {
        private String jobId;
        private String tableId;
        private RecordsProcessedSummary summary;
        private String jobRunId;
        private String taskId;

        private Builder() {
        }

        /**
         * Sets the ingest job ID and the table ID using the provided ingest job.
         *
         * @param  job the ingest job
         * @return     the builder
         */
        public Builder job(IngestJob job) {
            return jobId(job.getId())
                    .tableId(job.getTableId());
        }

        /**
         * Sets the ingest job ID.
         *
         * @param  jobId the ingest job ID
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
         * Sets the records processed summary.
         *
         * @param  summary the records processed summary
         * @return         the builder
         */
        public Builder summary(RecordsProcessedSummary summary) {
            this.summary = summary;
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

        public IngestJobFinishedEvent build() {
            return new IngestJobFinishedEvent(this);
        }
    }
}
