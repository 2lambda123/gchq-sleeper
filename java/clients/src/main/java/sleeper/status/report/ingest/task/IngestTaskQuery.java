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
package sleeper.status.report.ingest.task;

import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;

import java.util.List;

@FunctionalInterface
public interface IngestTaskQuery {
    IngestTaskQuery ALL = IngestTaskStatusStore::getAllTasks;

    List<IngestTaskStatus> run(IngestTaskStatusStore store);

    static IngestTaskQuery from(String type) {
        if ("-a".equals(type)) {
            return ALL;
        }
        throw new IllegalArgumentException("Unrecognised query type: " + type);
    }
}
