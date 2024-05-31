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

import sleeper.core.record.process.status.JobStatusUpdates;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRuns;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.core.record.process.status.TimeWindowQuery;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.compaction.job.status.CompactionJobStatusType.CREATED;

public class CompactionJobStatus {

    private final String jobId;
    private final CompactionJobCreatedStatus createdStatus;
    private final ProcessRuns jobRuns;
    private final Instant expiryDate;

    private CompactionJobStatus(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        createdStatus = Objects.requireNonNull(builder.createdStatus, "createdStatus must not be null");
        jobRuns = builder.jobRuns;
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static List<CompactionJobStatus> listFrom(Stream<ProcessStatusUpdateRecord> records) {
        return streamFrom(records).collect(Collectors.toList());
    }

    public static Stream<CompactionJobStatus> streamFrom(Stream<ProcessStatusUpdateRecord> records) {
        return JobStatusUpdates.streamFrom(records)
                .map(CompactionJobStatus::from)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private static Optional<CompactionJobStatus> from(JobStatusUpdates updates) {
        return updates.getFirstStatusUpdateOfType(CompactionJobCreatedStatus.class)
                .map(createdStatus -> builder()
                        .jobId(updates.getJobId())
                        .createdStatus(createdStatus)
                        .jobRuns(updates.getRuns())
                        .expiryDate(updates.getFirstRecord().getExpiryDate())
                        .build());
    }

    public Instant getCreateUpdateTime() {
        return createdStatus.getUpdateTime();
    }

    public String getPartitionId() {
        return createdStatus.getPartitionId();
    }

    public int getInputFilesCount() {
        return createdStatus.getInputFilesCount();
    }

    public boolean isStarted() {
        return jobRuns.isStarted();
    }

    public boolean isFinished() {
        return jobRuns.isFinished();
    }

    public Instant getExpiryDate() {
        return expiryDate;
    }

    public String getJobId() {
        return jobId;
    }

    public boolean isTaskIdAssigned(String taskId) {
        return jobRuns.isTaskIdAssigned(taskId);
    }

    public boolean isInPeriod(Instant windowStartTime, Instant windowEndTime) {
        TimeWindowQuery timeWindowQuery = new TimeWindowQuery(windowStartTime, windowEndTime);
        if (isFinished()) {
            return timeWindowQuery.isFinishedProcessInWindow(
                    createdStatus.getUpdateTime(), jobRuns.lastTime().orElseThrow());
        } else {
            return timeWindowQuery.isUnfinishedProcessInWindow(createdStatus.getUpdateTime());
        }
    }

    public List<ProcessRun> getJobRuns() {
        return jobRuns.getRunsLatestFirst();
    }

    public Optional<ProcessRun> getLatestRun() {
        return jobRuns.getLatestRun();
    }

    public CompactionJobStatusType getFurthestStatusType() {
        return jobRuns.getRunsLatestFirst().stream()
                .map(ProcessRun::getLatestUpdate)
                .map(CompactionJobStatusType::of)
                .max(Comparator.comparing(CompactionJobStatusType::getOrder))
                .orElse(CREATED);
    }

    public static final class Builder {
        private String jobId;
        private CompactionJobCreatedStatus createdStatus;
        private ProcessRuns jobRuns;
        private Instant expiryDate;

        private Builder() {
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder createdStatus(CompactionJobCreatedStatus createdStatus) {
            this.createdStatus = createdStatus;
            return this;
        }

        public Builder singleJobRun(ProcessRun jobRun) {
            return jobRunsLatestFirst(Collections.singletonList(jobRun));
        }

        public Builder jobRunsLatestFirst(List<ProcessRun> jobRunList) {
            return jobRuns(ProcessRuns.latestFirst(jobRunList));
        }

        public Builder jobRuns(ProcessRuns jobRuns) {
            this.jobRuns = jobRuns;
            return this;
        }

        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public CompactionJobStatus build() {
            return new CompactionJobStatus(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionJobStatus status = (CompactionJobStatus) o;
        return jobId.equals(status.jobId) && createdStatus.equals(status.createdStatus) && Objects.equals(jobRuns, status.jobRuns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, createdStatus, jobRuns);
    }

    @Override
    public String toString() {
        return "CompactionJobStatus{" +
                "jobId='" + jobId + '\'' +
                ", createdStatus=" + createdStatus +
                ", jobRuns=" + jobRuns +
                ", expiryDate=" + expiryDate +
                '}';
    }
}
