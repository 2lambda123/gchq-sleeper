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

package sleeper.clients.status.report.filestatus;

public class PartitionFileReferenceStats {
    private final Integer minReferences;
    private final Integer maxReferences;
    private final Double averageReferences;
    private final Integer totalReferences;

    public PartitionFileReferenceStats(Integer minRecords, Integer maxReferences, Double averageReferences, Integer totalReferences) {
        this.minReferences = minRecords;
        this.maxReferences = maxReferences;
        this.averageReferences = averageReferences;
        this.totalReferences = totalReferences;
    }

    public Integer getMinReferences() {
        return minReferences;
    }

    public Integer getMaxReferences() {
        return maxReferences;
    }

    public Double getAverageReferences() {
        return averageReferences;
    }

    public Integer getTotalReferences() {
        return totalReferences;
    }
}
