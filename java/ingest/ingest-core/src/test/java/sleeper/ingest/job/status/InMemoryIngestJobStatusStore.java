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
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.core.table.TableIdentity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.record.process.status.TestRunStatusUpdates.defaultUpdateTime;
import static sleeper.ingest.job.status.IngestJobStatusType.REJECTED;

public class InMemoryIngestJobStatusStore implements IngestJobStatusStore {
    private final Map<String, TableJobs> tableIdToJobs = new HashMap<>();

    @Override
    public void jobValidated(IngestJobValidatedEvent event) {
        tableIdToJobs.computeIfAbsent(event.getTableId(), tableId -> new TableJobs()).jobIdToUpdateRecords.computeIfAbsent(event.getJobId(), jobId -> new ArrayList<>())
                .add(ProcessStatusUpdateRecord.builder()
                        .jobId(event.getJobId())
                        .statusUpdate(event.toStatusUpdate(
                                defaultUpdateTime(event.getValidationTime())))
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    @Override
    public void jobStarted(IngestJobStartedEvent event) {
        tableIdToJobs.computeIfAbsent(event.getTableId(), tableId -> new TableJobs()).jobIdToUpdateRecords.computeIfAbsent(event.getJobId(), jobId -> new ArrayList<>())
                .add(ProcessStatusUpdateRecord.builder()
                        .jobId(event.getJobId())
                        .statusUpdate(IngestJobStartedStatus.withStartOfRun(event.isStartOfRun())
                                .inputFileCount(event.getFileCount())
                                .startTime(event.getStartTime())
                                .updateTime(defaultUpdateTime(event.getStartTime()))
                                .build())
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    @Override
    public void jobFinished(IngestJobFinishedEvent event) {
        RecordsProcessedSummary summary = event.getSummary();
        List<ProcessStatusUpdateRecord> jobRecords = tableJobs(event.getTableId())
                .map(jobs -> jobs.jobIdToUpdateRecords.get(event.getJobId()))
                .orElseThrow(() -> new IllegalStateException("Job not started: " + event.getJobId()));
        jobRecords.add(ProcessStatusUpdateRecord.builder()
                .jobId(event.getJobId())
                .statusUpdate(ProcessFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary))
                .jobRunId(event.getJobRunId())
                .taskId(event.getTaskId())
                .build());
    }

    @Override
    public Stream<IngestJobStatus> streamAllJobs(String tableId) {
        return IngestJobStatus.streamFrom(streamTableRecords(tableId));
    }

    @Override
    public List<IngestJobStatus> getInvalidJobs() {
        return streamAllJobs()
                .filter(status -> status.getFurthestStatusType().equals(REJECTED))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<IngestJobStatus> getJob(String jobId) {
        return streamAllJobs()
                .filter(status -> Objects.equals(jobId, status.getJobId()))
                .findFirst();
    }

    private Stream<IngestJobStatus> streamAllJobs() {
        return IngestJobStatus.streamFrom(tableIdToJobs.values().stream()
                .flatMap(TableJobs::streamAllRecords));
    }

    public Stream<ProcessStatusUpdateRecord> streamTableRecords(TableIdentity tableId) {
        return streamTableRecords(tableId.getTableUniqueId());
    }

    public Stream<ProcessStatusUpdateRecord> streamTableRecords(String tableId) {
        return tableJobs(tableId)
                .map(TableJobs::streamAllRecords)
                .orElse(Stream.empty());
    }

    private Optional<TableJobs> tableJobs(String tableId) {
        return Optional.ofNullable(tableIdToJobs.get(tableId));
    }

    private static class TableJobs {
        private final Map<String, List<ProcessStatusUpdateRecord>> jobIdToUpdateRecords = new HashMap<>();

        private Stream<ProcessStatusUpdateRecord> streamAllRecords() {
            return jobIdToUpdateRecords.values().stream().flatMap(List::stream);
        }
    }
}
