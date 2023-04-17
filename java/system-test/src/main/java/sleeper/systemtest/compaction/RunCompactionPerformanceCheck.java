/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.compaction;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.systemtest.SystemTestProperties;

public class RunCompactionPerformanceCheck {
    private final CompactionPerformanceResults expectedResults;
    private final CompactionPerformanceResults results;

    private RunCompactionPerformanceCheck(Builder builder) {
        expectedResults = builder.expectedResults;
        results = builder.results;
    }

    public static RunCompactionPerformanceCheck loadFrom(
            SystemTestProperties properties, TableProperties tableProperties,
            StateStore stateStore, CompactionJobStatusStore jobStatusStore) throws StateStoreException {
        return RunCompactionPerformanceCheck.builder()
                .expectedResults(CompactionPerformanceResults.loadExpected(properties, tableProperties))
                .results(CompactionPerformanceResults.loadActual(tableProperties, stateStore, jobStatusStore))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }


    public static final class Builder {
        private CompactionPerformanceResults expectedResults;
        private CompactionPerformanceResults results;

        public Builder() {
        }

        public Builder expectedResults(CompactionPerformanceResults expectedResults) {
            this.expectedResults = expectedResults;
            return this;
        }

        public Builder results(CompactionPerformanceResults results) {
            this.results = results;
            return this;
        }

        public RunCompactionPerformanceCheck build() {
            return new RunCompactionPerformanceCheck(this);
        }
    }
}
