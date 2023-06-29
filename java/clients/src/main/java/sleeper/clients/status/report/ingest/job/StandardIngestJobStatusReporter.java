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

import sleeper.clients.status.report.job.AverageRecordRateReport;
import sleeper.clients.status.report.job.StandardProcessRunReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriter;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.core.record.process.AverageRecordRate;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.ingest.job.status.*;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

public class StandardIngestJobStatusReporter implements IngestJobStatusReporter {

    private final TableField stateField;
    private final TableField jobIdField;
    private final TableField inputFilesCount;
    private final TableWriterFactory tableFactory;
    private final StandardProcessRunReporter runReporter;

    private final PrintStream out;

    public StandardIngestJobStatusReporter() {
        this(System.out);
    }

    public StandardIngestJobStatusReporter(PrintStream out) {
        this.out = out;
        TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
        stateField = tableFactoryBuilder.addField("STATE");
        jobIdField = tableFactoryBuilder.addField("JOB_ID");
        inputFilesCount = tableFactoryBuilder.addNumericField("INPUT_FILES");
        runReporter = new StandardProcessRunReporter(out, tableFactoryBuilder);
        tableFactory = tableFactoryBuilder.build();
    }

    @Override
    public void report(List<IngestJobStatus> statusList, JobQuery.Type query, IngestQueueMessages queueMessages,
                       Map<String, Integer> persistentEmrStepCount) {
        out.println();
        out.println("Ingest Job Status Report");
        out.println("------------------------");
        printSummary(statusList, query, queueMessages, persistentEmrStepCount);
        if (!query.equals(JobQuery.Type.DETAILED)) {
            tableFactory.tableBuilder()
                    .showFields(query != JobQuery.Type.UNFINISHED, runReporter.getFinishedFields())
                    .itemsAndSplittingWriter(statusList, this::writeJob)
                    .build().write(out);
        }
    }

    private void printSummary(List<IngestJobStatus> statusList, JobQuery.Type queryType,
                              IngestQueueMessages queueMessages, Map<String, Integer> persistentEmrStepCount) {
        if (queryType.equals(JobQuery.Type.DETAILED)) {
            printDetailedSummary(statusList);
        } else if (queryType.equals(JobQuery.Type.ALL)) {
            printAllSummary(statusList, queueMessages, persistentEmrStepCount);
        } else if (queryType.equals(JobQuery.Type.UNFINISHED)) {
            printUnfinishedSummary(statusList, queueMessages, persistentEmrStepCount);
        } else if (queryType.equals(JobQuery.Type.RANGE)) {
            printRangeSummary(statusList, queueMessages);
        }
    }

    private void printDetailedSummary(List<IngestJobStatus> statusList) {
        if (statusList.isEmpty()) {
            out.println("No job found with provided jobId");
            out.println("------------------------");
        } else {
            for (IngestJobStatus status : statusList) {
                if (status == null) {
                    out.println("No job found with provided jobId");
                } else {
                    printDetailedSummary(status);
                }
                out.println("------------------------");
            }
        }
    }

    private void printDetailedSummary(IngestJobStatus status) {
        out.printf("Details for job %s:%n", status.getJobId());
        out.printf("State: %s%n", status.getFurthestStatusType());
        out.printf("Number of input files: %d%n", status.getInputFilesCount());
        for (ProcessRun run : status.getJobRuns()) {
            printProcessJobRun(run);
        }
    }

    private void printProcessJobRun(ProcessRun run) {
        out.println();
        if (run.getTaskId() != null) {
            out.printf("Run on task %s%n", run.getTaskId());
        }
        run.getLastStatusOfType(IngestJobValidatedStatus.class)
                .ifPresent(this::printValidation);
        run.getLastStatusOfType(IngestJobStartedStatus.class)
                .ifPresent(started -> runReporter.printProcessJobRun(run, started));
    }

    private void printValidation(IngestJobValidatedStatus status) {
        out.printf("Validation Time: %s%n", status.getStartTime());
        out.printf("Validation Update Time: %s%n", status.getUpdateTime());
        if (status.isValid()) {
            out.println("Job was accepted");
        } else {
            out.println("Job was rejected with reasons:");
            IngestJobRejectedStatus rejectedStatus = (IngestJobRejectedStatus) status;
            rejectedStatus.getReasons().forEach(reason -> out.printf("- %s%n", reason));
        }
    }

    private void printAllSummary(List<IngestJobStatus> statusList, IngestQueueMessages queueMessages,
                                 Map<String, Integer> persistentEmrStepCount) {
        printUnfinishedSummary(statusList, queueMessages, persistentEmrStepCount);
        out.printf("Total jobs finished: %s%n", statusList.stream().filter(IngestJobStatus::isFinished).count());
        AverageRecordRateReport.printf("Average ingest rate: %s%n", recordRate(statusList), out);
    }

    private void printUnfinishedSummary(List<IngestJobStatus> statusList, IngestQueueMessages queueMessages,
                                        Map<String, Integer> persistentEmrStepCount) {
        queueMessages.print(out);
        printPendingEmrStepCount(persistentEmrStepCount);
        out.printf("Total jobs in progress: %s%n", statusList.stream().filter(status -> !status.isFinished()).count());
    }

    private void printPendingEmrStepCount(Map<String, Integer> stepCount) {
        if (!stepCount.isEmpty()) {
            out.printf("Total persistent EMR steps pending: %s%n", stepCount.getOrDefault("PENDING", 0));
        }
    }

    private void printRangeSummary(List<IngestJobStatus> statusList, IngestQueueMessages queueMessages) {
        queueMessages.print(out);
        out.printf("Total jobs in defined range: %d%n", statusList.size());
        AverageRecordRateReport.printf("Average ingest rate: %s%n", recordRate(statusList), out);
    }

    private static AverageRecordRate recordRate(List<IngestJobStatus> jobs) {
        return AverageRecordRate.of(jobs.stream()
                .flatMap(job -> job.getJobRuns().stream()));
    }

    private void writeJob(IngestJobStatus job, TableWriter.Builder table) {
        job.getJobRuns().forEach(run -> table.row(row -> {
            writeJobFields(job, row);
            row.value(stateField, getState(run));
            runReporter.writeRunFields(run, row);
        }));

    }

    private String getState(ProcessRun run) {
        if (run.getStartedStatus() instanceof IngestJobAcceptedStatus) {
            return "ACCEPTED";
        } else if (run.getStartedStatus() instanceof IngestJobRejectedStatus) {
            return "REJECTED";
        } else {
            return StandardProcessRunReporter.getState(run);
        }
    }

    private void writeJobFields(IngestJobStatus job, TableRow.Builder builder) {
        builder.value(jobIdField, job.getJobId())
                .value(inputFilesCount, job.getInputFilesCount());
    }
}
