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
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.AverageRecordRate;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.systemtest.SystemTestProperties;

import java.util.Objects;

import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_WRITERS;

public class CompactionPerformanceResults {

    public static final double TARGET_RECORDS_PER_SECOND = 330000;

    private final int numOfJobs;
    private final long numOfRecordsInRoot;
    private final double writeRate;

    private CompactionPerformanceResults(Builder builder) {
        numOfJobs = builder.numOfJobs;
        numOfRecordsInRoot = builder.numOfRecordsInRoot;
        writeRate = builder.writeRate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CompactionPerformanceResults loadExpected(SystemTestProperties properties) {
        int expectedRecordsInRoot = properties.getInt(NUMBER_OF_WRITERS)
                * properties.getInt(NUMBER_OF_RECORDS_PER_WRITER);
        return builder()
                .numOfJobs(1)
                .numOfRecordsInRoot(expectedRecordsInRoot)
                .writeRate(TARGET_RECORDS_PER_SECOND)
                .build();
    }

    public static CompactionPerformanceResults loadActual(
            StateStore stateStore, CompactionJobStatusStore jobStatusStore) throws StateStoreException {
        return builder()
                .numOfJobs(jobStatusStore.getAllJobs("system-test").size())
                .numOfRecordsInRoot(stateStore.getActiveFiles().stream()
                        .mapToLong(FileInfo::getNumberOfRecords).sum())
                .writeRate(AverageRecordRate.of(jobStatusStore.streamAllJobs("system-test")
                        .filter(CompactionJobStatus::isFinished)
                        .flatMap(job -> job.getJobRuns().stream())).getRecordsWrittenPerSecond())
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionPerformanceResults results = (CompactionPerformanceResults) o;
        return numOfJobs == results.numOfJobs
                && numOfRecordsInRoot == results.numOfRecordsInRoot
                && Double.compare(results.writeRate, writeRate) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numOfJobs, numOfRecordsInRoot, writeRate);
    }

    @Override
    public String toString() {
        return "CompactionPerformanceResults{" +
                "numOfJobs=" + numOfJobs +
                ", numOfRecordsInRoot=" + numOfRecordsInRoot +
                ", writeRate=" + writeRate +
                '}';
    }

    public static final class Builder {
        private int numOfJobs;
        private long numOfRecordsInRoot;
        private double writeRate;

        private Builder() {
        }

        public Builder numOfJobs(int numOfJobs) {
            this.numOfJobs = numOfJobs;
            return this;
        }

        public Builder numOfRecordsInRoot(long numOfRecordsInRoot) {
            this.numOfRecordsInRoot = numOfRecordsInRoot;
            return this;
        }

        public Builder writeRate(double writeRate) {
            this.writeRate = writeRate;
            return this;
        }

        public CompactionPerformanceResults build() {
            return new CompactionPerformanceResults(this);
        }
    }
}
