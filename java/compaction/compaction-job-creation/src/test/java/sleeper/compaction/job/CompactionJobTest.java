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
package sleeper.compaction.job;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompactionJobTest {

    @Test
    public void testEqualsAndHashCodeForNonSplittingJobWithNoIterator() {
        // Given
        CompactionJob compactionJob1 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile")
                .isSplittingJob(false)
                .partitionId("partition1").build();
        CompactionJob compactionJob2 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile")
                .isSplittingJob(false)
                .partitionId("partition1").build();
        CompactionJob compactionJob3 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-2")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile2")
                .isSplittingJob(false)
                .partitionId("partition1").build();
        CompactionJob compactionJob4 = CompactionJob.builder()
                .tableName("table2")
                .jobId("job-2")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile2")
                .isSplittingJob(false)
                .partitionId("partition1").build();

        // When
        boolean equals1 = compactionJob1.equals(compactionJob2);
        boolean equals2 = compactionJob1.equals(compactionJob3);
        boolean equals3 = compactionJob3.equals(compactionJob4);
        int hashCode1 = compactionJob1.hashCode();
        int hashCode2 = compactionJob2.hashCode();
        int hashCode3 = compactionJob3.hashCode();
        int hashCode4 = compactionJob4.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(equals3).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
        assertThat(hashCode4).isNotEqualTo(hashCode3);
    }

    @Test
    public void testEqualsAndHashCodeForNonSplittingJobWithIterator() {
        // Given
        CompactionJob compactionJob1 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile")
                .isSplittingJob(false)
                .partitionId("partition1")
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1").build();
        CompactionJob compactionJob2 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile")
                .isSplittingJob(false)
                .partitionId("partition1")
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1").build();
        CompactionJob compactionJob3 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile")
                .isSplittingJob(false)
                .partitionId("partition1")
                .iteratorClassName("Iterator2.class")
                .iteratorConfig("config1").build();

        // When
        boolean equals1 = compactionJob1.equals(compactionJob2);
        boolean equals2 = compactionJob1.equals(compactionJob3);
        int hashCode1 = compactionJob1.hashCode();
        int hashCode2 = compactionJob2.hashCode();
        int hashCode3 = compactionJob3.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashCodeForSplittingJobWithNoIterator() {
        // Given
        CompactionJob compactionJob1 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile")
                .isSplittingJob(true)
                .partitionId("partition1")
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .dimension(2).build();
        CompactionJob compactionJob2 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile")
                .isSplittingJob(true)
                .partitionId("partition1")
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .dimension(2).build();
        CompactionJob compactionJob3 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile1")
                .isSplittingJob(true)
                .partitionId("partition1")
                .childPartitions(Arrays.asList("childPartition2", "childPartition3"))
                .dimension(2).build();
        CompactionJob compactionJob4 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile1")
                .isSplittingJob(true)
                .partitionId("partition1")
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .dimension(1).build();

        // When
        boolean equals1 = compactionJob1.equals(compactionJob2);
        boolean equals2 = compactionJob1.equals(compactionJob3);
        boolean equals3 = compactionJob1.equals(compactionJob4);
        int hashCode1 = compactionJob1.hashCode();
        int hashCode2 = compactionJob2.hashCode();
        int hashCode3 = compactionJob3.hashCode();
        int hashCode4 = compactionJob4.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(equals3).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
        assertThat(hashCode4).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashCodeForSplittingJobWithIterator() {
        // Given
        CompactionJob compactionJob1 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile")
                .isSplittingJob(true)
                .partitionId("partition1")
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .dimension(2)
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1").build();
        CompactionJob compactionJob2 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile")
                .isSplittingJob(true)
                .partitionId("partition1")
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .dimension(2)
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1").build();
        CompactionJob compactionJob3 = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(Arrays.asList("file1", "file2", "file3"))
                .outputFile("outputFile1")
                .isSplittingJob(true)
                .partitionId("partition1")
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .dimension(2)
                .iteratorClassName("Iterator2.class")
                .iteratorConfig("config1").build();

        // When
        boolean equals1 = compactionJob1.equals(compactionJob2);
        boolean equals2 = compactionJob1.equals(compactionJob3);
        int hashCode1 = compactionJob1.hashCode();
        int hashCode2 = compactionJob2.hashCode();
        int hashCode3 = compactionJob3.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testShouldThrowOnDuplicateNames() {
        // Given
        List<String> names = Arrays.asList("file1", "file2", "file3", "file1");
        CompactionJob.Builder jobBuilder = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(names);

        // When / Then
        assertThatThrownBy(jobBuilder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testShouldThrowOnDuplicateNulls() {
        // Given
        List<String> names = new ArrayList<>();
        names.add(null);
        names.add(null);
        CompactionJob.Builder jobBuilder = CompactionJob.builder()
                .tableName("table")
                .jobId("job-1")
                .inputFiles(names);

        // When / Then
        assertThatThrownBy(jobBuilder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }
}
