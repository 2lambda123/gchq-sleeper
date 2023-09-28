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

package sleeper.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;
import java.util.Objects;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TableMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetrics.class);

    private final String instanceId;
    private final String tableName;
    private final int fileCount;
    private final long recordCount;
    private final int partitionCount;
    private final int leafPartitionCount;
    private final double averageActiveFilesPerPartition;

    private TableMetrics(Builder builder) {
        instanceId = builder.instanceId;
        tableName = builder.tableName;
        fileCount = builder.fileCount;
        recordCount = builder.recordCount;
        partitionCount = builder.partitionCount;
        leafPartitionCount = builder.leafPartitionCount;
        averageActiveFilesPerPartition = builder.averageActiveFilesPerPartition;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TableMetrics from(InstanceProperties instanceProperties, TableProperties tableProperties,
                                    StateStore stateStore) throws StateStoreException {
        String tableName = tableProperties.get(TABLE_NAME);

        LOGGER.info("Querying state store for table {} for partitions", tableName);
        List<Partition> partitions = stateStore.getAllPartitions();
        int partitionCount = partitions.size();
        int leafPartitionCount = (int) partitions.stream().filter(Partition::isLeafPartition).count();
        LOGGER.info("Found {} partitions and {} leaf partitions for table {}", partitionCount, leafPartitionCount, tableName);
        return TableMetrics.builder()
                .instanceId(instanceProperties.get(ID))
                .tableName(tableName)
                .partitionCount(partitionCount)
                .leafPartitionCount(leafPartitionCount)
                .build();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        TableMetrics that = (TableMetrics) object;
        return fileCount == that.fileCount && recordCount == that.recordCount && partitionCount == that.partitionCount && leafPartitionCount == that.leafPartitionCount && Double.compare(averageActiveFilesPerPartition, that.averageActiveFilesPerPartition) == 0 && Objects.equals(instanceId, that.instanceId) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, tableName, fileCount, recordCount, partitionCount, leafPartitionCount, averageActiveFilesPerPartition);
    }

    @Override
    public String toString() {
        return "TableMetrics{" +
                "instanceId='" + instanceId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", fileCount=" + fileCount +
                ", recordCount=" + recordCount +
                ", partitionCount=" + partitionCount +
                ", leafPartitionCount=" + leafPartitionCount +
                ", averageActiveFilesPerPartition=" + averageActiveFilesPerPartition +
                '}';
    }

    public static final class Builder {
        private String instanceId;
        private String tableName;
        private int fileCount;
        private long recordCount;
        private int partitionCount;
        private int leafPartitionCount;
        private double averageActiveFilesPerPartition;

        private Builder() {
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder fileCount(int fileCount) {
            this.fileCount = fileCount;
            return this;
        }

        public Builder recordCount(long recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public Builder partitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        public Builder leafPartitionCount(int leafPartitionCount) {
            this.leafPartitionCount = leafPartitionCount;
            return this;
        }

        public Builder averageActiveFilesPerPartition(double averageActiveFilesPerPartition) {
            this.averageActiveFilesPerPartition = averageActiveFilesPerPartition;
            return this;
        }

        public TableMetrics build() {
            return new TableMetrics(this);
        }
    }
}
