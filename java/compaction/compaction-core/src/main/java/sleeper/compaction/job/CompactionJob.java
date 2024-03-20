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
package sleeper.compaction.job;

import sleeper.core.statestore.FileReference;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Contains the definition of a compaction job. This includes the ID of the job,
 * a list of the input files, the output file, and the ID of the partition.
 * <p>
 * This should fully define the job to be performed, so that no further queries of
 * the state store should be required in order to start it.
 */
public class CompactionJob {
    private final String tableId;
    private final String jobId;
    private final List<String> inputFiles;
    private final String outputFile;
    private final String partitionId;
    private final String iteratorClassName;
    private final String iteratorConfig;

    private CompactionJob(Builder builder) {
        tableId = builder.tableId;
        jobId = builder.jobId;
        inputFiles = builder.inputFiles;
        outputFile = builder.outputFile;
        partitionId = builder.partitionId;
        iteratorClassName = builder.iteratorClassName;
        iteratorConfig = builder.iteratorConfig;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTableId() {
        return tableId;
    }

    public String getId() {
        return jobId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public List<String> getInputFiles() {
        return inputFiles;
    }

    /**
     * Checks that there are no duplicate entries present in the list of files.
     *
     * @param  <T>                      generic type of list
     * @param  files                    list of entries to check
     * @throws IllegalArgumentException if there are any duplicate entries
     */
    private static <T> void checkDuplicates(List<T> files) {
        if (files.stream()
                .distinct()
                .count() != files.size()) {
            throw new IllegalArgumentException("Duplicate entry present in file list");
        }
    }

    public String getOutputFile() {
        return outputFile;
    }

    public String getIteratorClassName() {
        return iteratorClassName;
    }

    public String getIteratorConfig() {
        return iteratorConfig;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        CompactionJob that = (CompactionJob) object;
        return Objects.equals(tableId, that.tableId) && Objects.equals(jobId, that.jobId)
                && Objects.equals(inputFiles, that.inputFiles) && Objects.equals(outputFile, that.outputFile)
                && Objects.equals(partitionId, that.partitionId) && Objects.equals(iteratorClassName, that.iteratorClassName)
                && Objects.equals(iteratorConfig, that.iteratorConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, jobId, inputFiles, outputFile, partitionId,
                iteratorClassName, iteratorConfig);
    }

    @Override
    public String toString() {
        return "CompactionJob{" +
                "tableId='" + tableId + '\'' +
                ", jobId='" + jobId + '\'' +
                ", inputFiles=" + inputFiles +
                ", outputFile='" + outputFile + '\'' +
                ", partitionId='" + partitionId + '\'' +
                ", iteratorClassName='" + iteratorClassName + '\'' +
                ", iteratorConfig='" + iteratorConfig + '\'' +
                '}';
    }

    public static final class Builder {
        private String tableId;
        private String jobId;
        private List<String> inputFiles;
        private String outputFile;
        private String partitionId;
        private String iteratorClassName;
        private String iteratorConfig;

        private Builder() {
        }

        public CompactionJob build() {
            Objects.requireNonNull(tableId, "tableId must not be null");
            Objects.requireNonNull(jobId, "jobId must not be null");
            Objects.requireNonNull(inputFiles, "inputFiles must not be null");
            Objects.requireNonNull(outputFile, "outputFile must not be null");
            Objects.requireNonNull(partitionId, "partitionId must not be null");
            checkDuplicates(inputFiles);
            return new CompactionJob(this);
        }

        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder inputFiles(List<String> inputFiles) {
            this.inputFiles = inputFiles;
            return this;
        }

        public Builder inputFileReferences(List<FileReference> inputFiles) {
            return inputFiles(inputFiles.stream()
                    .map(FileReference::getFilename)
                    .collect(Collectors.toList()));
        }

        public Builder outputFile(String outputFile) {
            this.outputFile = outputFile;
            return this;
        }

        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder iteratorClassName(String iteratorClassName) {
            this.iteratorClassName = iteratorClassName;
            return this;
        }

        public Builder iteratorConfig(String iteratorConfig) {
            this.iteratorConfig = iteratorConfig;
            return this;
        }
    }
}
