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

package sleeper.bulkimport.job.runner;

import org.junit.jupiter.api.Test;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.WriteToMemoryIngestJobStatusStore;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStore;
import sleeper.statestore.inmemory.StateStoreTestHelper;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestJob;
import static sleeper.statestore.FileInfoTestData.defaultFileOnRootPartitionWithRecords;

class BulkImportJobReportingTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition(schema);
    private final IngestJobStatusStore statusStore = new WriteToMemoryIngestJobStatusStore();

    @Test
    void shouldReportJobFinished() throws Exception {
        // Given
        BulkImportJob job = singleFileImportJob();
        Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
        Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
        List<FileInfo> outputFiles = List.of(
                defaultFileOnRootPartitionWithRecords("test-output.parquet", 100));

        // When
        runJob(job, "test-task", startTime, finishTime, outputFiles);

        // Then
        assertThat(allJobsReported())
                .containsExactly(finishedIngestJob(job.toIngestJob(), "test-task",
                        summary(startTime, finishTime, 100, 100)));
    }

    private void runJob(BulkImportJob job, String taskId,
                        Instant startTime, Instant finishTime, List<FileInfo> outputFiles) throws Exception {
        BulkImportJobOutput output = new BulkImportJobOutput(outputFiles, () -> {
        });
        BulkImportJobDriver driver = new BulkImportJobDriver(bulkImportJob -> output,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                statusStore, List.of(startTime, finishTime).iterator()::next);
        driver.run(job, taskId);
    }

    private BulkImportJob singleFileImportJob() {
        return BulkImportJob.builder()
                .id("test-job")
                .tableName(tableProperties.get(TABLE_NAME))
                .files(List.of("test.parquet")).build();
    }

    private List<IngestJobStatus> allJobsReported() {
        return statusStore.getAllJobs(tableProperties.get(TABLE_NAME));
    }
}
