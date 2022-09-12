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
package sleeper.status.report.filestatus;

import org.junit.Test;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStoreException;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StandardFileStatusReporterRecordCountTest {

    @Test
    public void shouldReportExactCountWhenLowerThan1K() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123" + System.lineSeparator());
    }

    @Test
    public void shouldReportKCountWhenLowerThan1M() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123456);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123K (123,456)" + System.lineSeparator());
    }

    @Test
    public void shouldReportMCountWhenLowerThan1G() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_456_789);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123M (123,456,789)" + System.lineSeparator());
    }

    @Test
    public void shouldReportGCountWhenHigherThan1G() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_123_456_789L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123G (123,123,456,789)" + System.lineSeparator());
    }

    @Test
    public void shouldReportTCountWhenHigherThan1T() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_456_123_456_789L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 123T (123,456,123,456,789)" + System.lineSeparator());
    }

    @Test
    public void shouldReportTCountWhenHigherThan1000T() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(1_234_123_123_456_789L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 1,234T (1,234,123,123,456,789)" + System.lineSeparator());
    }

    @Test
    public void shouldRoundUpKCount() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_500);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 124K (123,500)" + System.lineSeparator());
    }

    @Test
    public void shouldRoundUpMCount() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_500_000);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 124M (123,500,000)" + System.lineSeparator());
    }

    @Test
    public void shouldRoundUpGCount() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_500_000_000L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 124G (123,500,000,000)" + System.lineSeparator());
    }

    @Test
    public void shouldRoundUpTCount() throws Exception {
        // Given
        FileStatus status = statusWithRecordCount(123_500_000_000_000L);

        // When / Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .contains("Total number of records in all active files = 124T (123,500,000,000,000)" + System.lineSeparator());
    }

    private static FileStatus statusWithRecordCount(long recordCount) throws StateStoreException {
        Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct();
        FileStatusReportTestHelper fileStatusReportTestHelper = new FileStatusReportTestHelper(schema, partitions, lastStateStoreUpdate);
        List<FileInfo> activeFiles = Collections.singletonList(
                fileStatusReportTestHelper.leafFile(recordCount, "arthur", "ford"));

        return FileStatusCollector.run(StateStoreSnapshot.builder()
                .partitions(partitions).active(activeFiles)
                .readyForGC(StateStoreReadyForGC.none())
                .build());
    }
}
