/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest.task;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStartedStatus;

import java.time.Instant;
import java.util.Objects;
import java.util.stream.Stream;

public class IngestTaskStatus {
    private final String taskId;
    private final Instant startTime;
    private final IngestTaskFinishedStatus finishedStatus;
    private final Instant expiryDate; // Set by database (null before status is saved)

    private IngestTaskStatus(Builder builder) {
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        startTime = Objects.requireNonNull(builder.startTime, "startTime must not be null");
        finishedStatus = builder.finishedStatus;
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTaskId() {
        return taskId;
    }

    public IngestTaskFinishedStatus getFinishedStatus() {
        return finishedStatus;
    }

    public boolean isInPeriod(Instant startTime, Instant endTime) {
        return startTime.isBefore(getLastTime())
                && endTime.isAfter(getStartTime());
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getFinishTime() {
        if (isFinished()) {
            return finishedStatus.getFinishTime();
        } else {
            return null;
        }
    }

    private Instant getLastTime() {
        if (isFinished()) {
            return finishedStatus.getFinishTime();
        } else {
            return startTime;
        }
    }

    public boolean isFinished() {
        return finishedStatus != null;
    }

    public Integer getJobRunsOrNull() {
        if (isFinished()) {
            return finishedStatus.getTotalJobRuns();
        } else {
            return null;
        }
    }

    public int getJobRuns() {
        if (isFinished()) {
            return finishedStatus.getTotalJobRuns();
        } else {
            return 0;
        }
    }

    public ProcessRun asProcessRun() {
        return ProcessRun.builder().taskId(taskId)
                .startedStatus(ProcessStartedStatus.updateAndStartTime(getStartTime(), getStartTime()))
                .finishedStatus(asProcessFinishedStatus())
                .build();
    }

    private ProcessFinishedStatus asProcessFinishedStatus() {
        if (finishedStatus == null) {
            return null;
        }
        return ProcessFinishedStatus.updateTimeAndSummary(
                finishedStatus.getFinishTime(),
                finishedStatus.asSummary(getStartTime()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestTaskStatus that = (IngestTaskStatus) o;
        return taskId.equals(that.taskId)
                && startTime.equals(that.startTime)
                && Objects.equals(finishedStatus, that.finishedStatus)
                && Objects.equals(expiryDate, that.expiryDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, startTime, finishedStatus, expiryDate);
    }

    @Override
    public String toString() {
        return "IngestTaskStatus{" +
                "taskId='" + taskId + '\'' +
                ", startTime=" + startTime +
                ", finishedStatus=" + finishedStatus +
                ", expiryDate=" + expiryDate +
                '}';
    }

    public static final class Builder {
        private String taskId;
        private Instant startTime;
        private IngestTaskFinishedStatus finishedStatus;
        private Instant expiryDate;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder startTime(long startTime) {
            return startTime(Instant.ofEpochMilli(startTime));
        }

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder finishedStatus(IngestTaskFinishedStatus finishedStatus) {
            this.finishedStatus = finishedStatus;
            return this;
        }

        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public Builder finished(IngestTaskFinishedStatus.Builder taskFinishedBuilder, long finishTime) {
            return finished(taskFinishedBuilder, Instant.ofEpochMilli(finishTime));
        }

        public Builder finished(IngestTaskFinishedStatus.Builder taskFinishedBuilder, Instant finishTime) {
            return finishedStatus(taskFinishedBuilder
                    .finish(startTime, finishTime)
                    .build());
        }

        public Builder finished(Instant finishTime, Stream<RecordsProcessedSummary> jobSummaries) {
            IngestTaskFinishedStatus.Builder builder = IngestTaskFinishedStatus.builder();
            jobSummaries.forEach(builder::addJobSummary);
            return finishedStatus(builder.finish(startTime, finishTime).build());
        }

        public String getTaskId() {
            return this.taskId;
        }

        public IngestTaskStatus build() {
            return new IngestTaskStatus(this);
        }
    }
}
