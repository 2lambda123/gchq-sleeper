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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.DelegatingStateStore;
import sleeper.statestore.FileInfoFactory;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.inmemory.FixedPartitionStore;
import sleeper.statestore.inmemory.InMemoryFileInfoStore;
import sleeper.systemtest.SystemTestProperties;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.systemtest.SystemTestPropertiesTestHelper.createTestSystemTestProperties;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_WRITERS;

class CompactionPerformanceValidatorTest {

    private static final String TEST_TABLE_NAME = "test-table";
    private final Schema schema = schemaWithKey("key", new StringType());
    private final StateStore stateStore = new DelegatingStateStore(
            new InMemoryFileInfoStore(), new FixedPartitionStore(schema));

    private final FileInfoFactory fileInfoFactory = createFileInfoFactory();
    private final CompactionJobStatusStore jobStatusStore = new CompactionJobStatusStoreInMemory();
    private final CompactionJobTestDataHelper dataHelper = CompactionJobTestDataHelper.forTable(TEST_TABLE_NAME);
    private final SystemTestProperties testProperties = createTestSystemTestProperties();
    private final TableProperties tableProperties = createTableProperties(testProperties);

    @Nested
    @DisplayName("Validate number of jobs that were run")
    class ValidateNumberOfJobs {

        @Test
        void shouldCalculateNumberOfJobsWhenNumberOfWritersIsSmallerThanBatchSize() {
            // Given
            testProperties.set(NUMBER_OF_WRITERS, "1");
            tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "5");

            // When
            CompactionPerformanceValidator validator = createValidator();

            // Then
            assertThat(validator.getNumberOfJobsExpected())
                    .isOne();
        }

        @Test
        void shouldCalculateNumberOfJobsWhenNumberOfWritersIsIndivisibleByBatchSize() {
            // Given
            testProperties.set(NUMBER_OF_WRITERS, "6");
            tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "5");

            // When
            CompactionPerformanceValidator validator = createValidator();

            // Then
            assertThat(validator.getNumberOfJobsExpected())
                    .isEqualTo(2);
        }

        @Test
        void shouldCalculateNumberOfJobsWhenNumberOfWritersIsDivisibleByBatchSize() {
            // Given
            testProperties.set(NUMBER_OF_WRITERS, "10");
            tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "5");

            // When
            CompactionPerformanceValidator validator = createValidator();

            // Then
            assertThat(validator.getNumberOfJobsExpected())
                    .isEqualTo(2);
        }

        @Test
        void shouldPassWhenSingleJobWasRunWithAllRecords() throws Exception {
            // Given
            testProperties.set(NUMBER_OF_WRITERS, "1");
            testProperties.set(NUMBER_OF_RECORDS_PER_WRITER, "10");

            jobFinishedWithNumberOfRecords(10);
            // When
            CompactionPerformanceResults results = loadResults();
            CompactionPerformanceValidator validator = createValidator();

            // Then
            assertThatCode(() -> validator.test(results)).doesNotThrowAnyException();
        }

        @Test
        void shouldFailWhenMultipleJobsWereRunButOneJobWasExpected() throws Exception {
            // Given
            testProperties.set(NUMBER_OF_WRITERS, "1");
            testProperties.set(NUMBER_OF_RECORDS_PER_WRITER, "10");

            jobFinishedWithNumberOfRecords(5);
            jobFinishedWithNumberOfRecords(5);
            // When
            CompactionPerformanceResults results = loadResults();
            CompactionPerformanceValidator validator = createValidator();

            // Then
            assertThatThrownBy(() -> validator.test(results))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Actual number of compaction jobs 2 did not match expected value 1");
        }
    }

    @Nested
    @DisplayName("Validate number of records that were output")
    class ValidateNumberOfRecords {

        @Test
        void shouldCalculateNumberOfRecordsExpectedWhenMultipleWritersGenerateMultipleRecords() {
            // Given
            testProperties.set(NUMBER_OF_WRITERS, "3");
            testProperties.set(NUMBER_OF_RECORDS_PER_WRITER, "10");

            // When
            CompactionPerformanceValidator validator = createValidator();

            // Then
            assertThat(validator.getNumberOfRecordsExpected())
                    .isEqualTo(30);
        }

        @Test
        void shouldFailWhenWhenSingleJobWasRunWithLessRecordsThanExpected() throws Exception {
            // Given
            testProperties.set(NUMBER_OF_WRITERS, "1");
            testProperties.set(NUMBER_OF_RECORDS_PER_WRITER, "10");

            jobFinishedWithNumberOfRecords(5);

            // When
            CompactionPerformanceResults results = loadResults();
            CompactionPerformanceValidator validator = createValidator();

            // Then
            assertThatThrownBy(() -> validator.test(results))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Actual number of records 5 did not match expected value 10");
        }
    }

    private void jobFinishedWithNumberOfRecords(int numberOfRecords) throws StateStoreException {
        CompactionJob job = reportFinishedJob(summary(Instant.parse("2023-04-17T16:15:42Z"), Duration.ofMinutes(1), numberOfRecords, numberOfRecords));
        stateStore.addFile(fileInfoFactory.rootFile(job.getId(), numberOfRecords, "aaa", "zzz"));
    }

    private FileInfoFactory createFileInfoFactory() {
        try {
            return new FileInfoFactory(schema, stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new IllegalStateException(e);
        }
    }

    private static TableProperties createTableProperties(SystemTestProperties properties) {
        TableProperties tableProperties = new TableProperties(properties);
        tableProperties.set(TableProperty.TABLE_NAME, TEST_TABLE_NAME);
        tableProperties.set(TableProperty.COMPACTION_FILES_BATCH_SIZE, "1");
        return tableProperties;
    }

    private CompactionPerformanceValidator createValidator() {
        return CompactionPerformanceValidator.from(testProperties, tableProperties);
    }

    private CompactionPerformanceResults loadResults() throws Exception {
        return CompactionPerformanceResults.loadActual(tableProperties, stateStore, jobStatusStore);
    }

    private CompactionJob reportFinishedJob(RecordsProcessedSummary summary) {
        return dataHelper.reportFinishedJob(summary, jobStatusStore);
    }

}
