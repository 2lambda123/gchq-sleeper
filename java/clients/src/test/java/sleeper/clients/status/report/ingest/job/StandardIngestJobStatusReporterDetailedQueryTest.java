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

package sleeper.clients.status.report.ingest.job;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatus;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.*;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestHelper.getStandardReport;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestHelper.replaceBracketedJobIds;
import static sleeper.clients.testutil.ClientTestUtils.example;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.*;
import static sleeper.ingest.job.status.IngestJobStatusTestData.*;

public class StandardIngestJobStatusReporterDetailedQueryTest {
    @Test
    public void shouldReportNoIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> noJobs = Collections.emptyList();

        // When / Then
        assertThat(getStandardReport(JobQuery.Type.DETAILED, noJobs, 0)).isEqualTo(
                example("reports/ingest/job/standard/detailed/noJobFound.txt"));
    }

    @Test
    public void shouldReportMixedIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> mixedJobs = mixedJobStatuses();

        // When / Then
        assertThat(getStandardReport(JobQuery.Type.DETAILED, mixedJobs, 0)).isEqualTo(
                replaceBracketedJobIds(mixedJobs, example("reports/ingest/job/standard/detailed/mixedJobs.txt")));
    }

    @Test
    public void shouldReportJobWithMultipleRuns() throws Exception {
        // Given
        List<IngestJobStatus> jobWithMultipleRuns = jobWithMultipleRuns();

        // When / Then
        assertThat(getStandardReport(JobQuery.Type.DETAILED, jobWithMultipleRuns, 0)).isEqualTo(
                replaceBracketedJobIds(jobWithMultipleRuns, example("reports/ingest/job/standard/detailed/jobWithMultipleRuns.txt")));
    }

    @Test
    public void shouldReportJobsWithLargeAndDecimalStatistics() throws Exception {
        // Given
        List<IngestJobStatus> jobsWithLargeAndDecimalStatistics = jobsWithLargeAndDecimalStatistics();

        // When / Then
        assertThat(getStandardReport(JobQuery.Type.DETAILED, jobsWithLargeAndDecimalStatistics, 0)).isEqualTo(
                replaceBracketedJobIds(jobsWithLargeAndDecimalStatistics, example("reports/ingest/job/standard/detailed/jobsWithLargeAndDecimalStatistics.txt")));
    }

    @Nested
    @DisplayName("Bulk Import job reporting")
    class BulkImportJobReporting {
        @Test
        void shouldReportPendingJobWithValidationAccepted() throws Exception {
            // Given
            List<IngestJobStatus> acceptedJob = acceptedJob();

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, acceptedJob, 0)).isEqualTo(
                    example("reports/ingest/job/standard/detailed/bulkImport/acceptedJob.txt"));
        }

        @Test
        void shouldReportStartedJobWithValidationAccepted() throws Exception {
            // Given
            List<IngestJobStatus> acceptedJob = acceptedJobWhichStarted();

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, acceptedJob, 0)).isEqualTo(
                    example("reports/ingest/job/standard/detailed/bulkImport/acceptedJobWhichStarted.txt"));
        }

        @Test
        void shouldReportRejectedJobWithOneReason() throws Exception {
            // Given
            List<IngestJobStatus> rejectedJob = rejectedJobWithOneReason();

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, rejectedJob, 0)).isEqualTo(
                    example("reports/ingest/job/standard/detailed/bulkImport/rejectedJobWithOneReason.txt"));
        }

        @Test
        void shouldReportRejectedJobWithMultipleReasons() throws Exception {
            // Given
            List<IngestJobStatus> rejectedJob = rejectedJobWithMultipleReasons();

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, rejectedJob, 0)).isEqualTo(
                    example("reports/ingest/job/standard/detailed/bulkImport/rejectedJobWithMultipleReasons.txt"));
        }

        @Test
        void shouldReportJobAcceptedThenRejected() throws Exception {
            // Given
            IngestJob job = createJob(1, 2);
            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("run-1",
                            acceptedStatusUpdate(job, Instant.parse("2023-06-05T17:20:00Z"))),
                    forNoRunNoTask(
                            rejectedStatusUpdate(job, Instant.parse("2023-06-05T17:30:00Z")))));

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, List.of(status), 0)).isEqualTo(
                    example("reports/ingest/job/standard/detailed/bulkImport/acceptedThenRejectedJob.txt"));
        }
    }
}
