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
package sleeper.core.record.process.status;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JobStatusUpdates {

    private final String jobId;
    private final List<ProcessStatusUpdateRecord> recordsLatestFirst;
    private final ProcessRuns runs;

    private JobStatusUpdates(
            String jobId, List<ProcessStatusUpdateRecord> recordsLatestFirst, ProcessRuns runs) {
        this.jobId = jobId;
        this.recordsLatestFirst = recordsLatestFirst;
        this.runs = runs;
    }

    public static JobStatusUpdates from(String jobId, List<ProcessStatusUpdateRecord> records) {
        List<ProcessStatusUpdateRecord> recordsLatestFirst = orderLatestFirst(records);
        ProcessRuns runs = ProcessRuns.fromRecordsLatestFirst(recordsLatestFirst);
        return new JobStatusUpdates(jobId, recordsLatestFirst, runs);
    }

    public static Stream<JobStatusUpdates> from(Stream<ProcessStatusUpdateRecord> records) {
        JobStatusesBuilder builder = new JobStatusesBuilder();
        records.forEach(builder::update);
        return builder.stream();
    }

    public String getJobId() {
        return jobId;
    }

    public ProcessStatusUpdateRecord getFirstRecord() {
        return recordsLatestFirst.get(recordsLatestFirst.size() - 1);
    }

    public ProcessStatusUpdateRecord getLastRecord() {
        return recordsLatestFirst.get(0);
    }

    public ProcessRuns getRuns() {
        return runs;
    }

    private static List<ProcessStatusUpdateRecord> orderLatestFirst(List<ProcessStatusUpdateRecord> records) {
        return records.stream()
                .sorted(Comparator.comparing(ProcessStatusUpdateRecord::getUpdateTime).reversed())
                .collect(Collectors.toList());
    }
}
