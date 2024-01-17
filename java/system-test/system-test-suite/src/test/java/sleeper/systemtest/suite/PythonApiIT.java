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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.util.CommandFailedException;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.dsl.reports.SystemTestReports;
import sleeper.systemtest.suite.testutil.AfterTestPurgeQueues;
import sleeper.systemtest.suite.testutil.AfterTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class PythonApiIT {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setup(SleeperSystemTest sleeper) {
        sleeper.connectToInstance(MAIN);
    }

    @Nested
    @DisplayName("Ingest files")
    class IngestFiles {

        @BeforeEach
        void setup(AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) {
            reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
            purgeQueues.purgeIfTestFailed(INGEST_JOB_QUEUE_URL);
        }

        @Test
        void shouldBatchWriteOneFile(SleeperSystemTest sleeper) throws IOException, InterruptedException {
            // Given
            sleeper.localFiles(tempDir)
                    .createWithNumberedRecords("file.parquet", LongStream.range(0, 100));

            // When
            sleeper.pythonApi()
                    .ingestByQueue().uploadingLocalFile(tempDir, "file.parquet")
                    .invokeTask().waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
            assertThat(sleeper.tableFiles().active()).hasSize(1);
        }

        @Test
        void shouldIngestTwoFilesFromS3(SleeperSystemTest sleeper) throws IOException, InterruptedException {
            // Given
            sleeper.sourceFiles()
                    .createWithNumberedRecords("file1.parquet", LongStream.range(0, 100))
                    .createWithNumberedRecords("file2.parquet", LongStream.range(100, 200));

            // When
            sleeper.pythonApi()
                    .ingestByQueue().fromS3("file1.parquet", "file2.parquet")
                    .invokeTask().waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 200)));
            assertThat(sleeper.tableFiles().active()).hasSize(1);
        }

        @Test
        void shouldIngestDirectoryFromS3(SleeperSystemTest sleeper) throws IOException, InterruptedException {
            // Given
            sleeper.sourceFiles()
                    .createWithNumberedRecords("test-dir/file1.parquet", LongStream.range(0, 100))
                    .createWithNumberedRecords("test-dir/file2.parquet", LongStream.range(100, 200));

            // When
            sleeper.pythonApi()
                    .ingestByQueue().fromS3("test-dir")
                    .invokeTask().waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 200)));
            assertThat(sleeper.tableFiles().active()).hasSize(1);
        }
    }

    @Nested
    @DisplayName("Bulk import files")
    class BulkImportFiles {

        @BeforeEach
        void setup(AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) {
            reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
            purgeQueues.purgeIfTestFailed(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL);
        }

        @Test
        void shouldBulkImportFilesFromS3(SleeperSystemTest sleeper) throws IOException, InterruptedException {
            // Given
            sleeper.updateTableProperties(Map.of(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1"));
            sleeper.sourceFiles()
                    .createWithNumberedRecords("file1.parquet", LongStream.range(0, 100))
                    .createWithNumberedRecords("file2.parquet", LongStream.range(100, 200));

            // When
            sleeper.pythonApi()
                    .bulkImport().fromS3("file1.parquet", "file2.parquet")
                    .waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 200)));
            assertThat(sleeper.tableFiles().active()).hasSize(1);
        }
    }

    @Nested
    @DisplayName("Run SQS query")
    class RunSQSQuery {

        @BeforeEach
        void setup(AfterTestPurgeQueues purgeQueues) {
            purgeQueues.purgeIfTestFailed(QUERY_QUEUE_URL);
        }

        @AfterEach
        void tearDown(SleeperSystemTest sleeper) {
            sleeper.query().emptyResultsBucket();
        }

        @Test
        void shouldRunExactKeyQuery(SleeperSystemTest sleeper) throws IOException, InterruptedException {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.pythonApi()
                    .query(tempDir).exactKeys("key",
                            "row-0000000000000000001",
                            "row-0000000000000000002")
                    .results())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.rangeClosed(1, 2)));
        }

        @Test
        void shouldRunRangeKeyQuery(SleeperSystemTest sleeper) throws IOException, InterruptedException {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.pythonApi()
                    .query(tempDir).range("key",
                            "row-0000000000000000010",
                            "row-0000000000000000020")
                    .results())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(10, 20)));
        }

        @Test
        void shouldRunRangeKeyQueryWithMinAndMaxInclusive(SleeperSystemTest sleeper) throws IOException, InterruptedException {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.pythonApi()
                    .query(tempDir).range("key",
                            "row-0000000000000000010", true,
                            "row-0000000000000000020", true)
                    .results())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.rangeClosed(10, 20)));
        }

        @Test
        void shouldFailToRunRangeKeyQueryWithNonExistentTable(SleeperSystemTest sleeper) {
            // When/Then
            assertThatThrownBy(() -> sleeper.pythonApi()
                    .query(tempDir).range("key", "not-a-table",
                            "row-0000000000000000010",
                            "row-0000000000000000020")
                    .results())
                    .isInstanceOf(CommandFailedException.class);
        }

        @Test
        void shouldFailToRunRangeKeyQueryWithNonExistentKey(SleeperSystemTest sleeper) {
            // When/Then
            assertThatThrownBy(() -> sleeper.pythonApi()
                    .query(tempDir).range("not-a-key",
                            "row-0000000000000000010",
                            "row-0000000000000000020")
                    .results())
                    .isInstanceOf(CommandFailedException.class);
        }
    }
}
