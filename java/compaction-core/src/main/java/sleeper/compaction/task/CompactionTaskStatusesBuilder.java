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
package sleeper.compaction.task;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionTaskStatusesBuilder {
    private final Map<String, CompactionTaskStartedStatus> startedById = new HashMap<>(); // Order by task ID for output
    private final Map<String, CompactionTaskFinishedStatus> finishedById = new HashMap<>();
    private final Map<String, Instant> expiryDateById = new HashMap<>();
    private final Map<String, CompactionTaskType> typeById = new HashMap<>();

    public CompactionTaskStatusesBuilder taskStarted(
            String taskId, CompactionTaskType type, Instant startTime) {
        startedById.put(taskId, new CompactionTaskStartedStatus(startTime));
        typeById.put(taskId, type);
        return this;
    }

    public CompactionTaskStatusesBuilder taskFinished(
            String taskId, CompactionTaskFinishedStatus finishedStatus) {
        finishedById.put(taskId, finishedStatus);
        return this;
    }

    public CompactionTaskStatusesBuilder expiryDate(
            String taskId, Instant expiryDate) {
        expiryDateById.put(taskId, expiryDate);
        return this;
    }

    public Stream<CompactionTaskStatus> stream() {
        return startedById.entrySet().stream()
                .map(entry -> fullStatus(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing(CompactionTaskStatus::getStartTime).reversed());
    }

    public List<CompactionTaskStatus> build() {
        return stream().collect(Collectors.toList());
    }

    private CompactionTaskStatus fullStatus(String taskId, CompactionTaskStartedStatus startedStatus) {
        return CompactionTaskStatus.builder().taskId(taskId).type(typeById.get(taskId))
                .startedStatus(startedStatus)
                .finishedStatus(finishedById.get(taskId))
                .expiryDate(expiryDateById.get(taskId))
                .build();
    }
}
