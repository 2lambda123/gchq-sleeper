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

package sleeper.status.report.compaction.job;

import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.AverageRecordRate;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.status.report.StandardProcessStatusReporter;
import sleeper.status.report.query.JobQuery;
import sleeper.status.report.table.TableField;
import sleeper.status.report.table.TableRow;
import sleeper.status.report.table.TableWriter;
import sleeper.status.report.table.TableWriterFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.status.report.StandardProcessStatusReporter.STATE_FINISHED;
import static sleeper.status.report.StandardProcessStatusReporter.STATE_IN_PROGRESS;
import static sleeper.status.report.StandardProcessStatusReporter.formatDecimal;

public class StandardCompactionJobStatusReporter implements CompactionJobStatusReporter {

    private final TableField stateField;
    private final TableField createTimeField;
    private final TableField jobIdField;
    private final TableField partitionIdField;
    private final TableField typeField;
    private final StandardProcessStatusReporter standardProcessStatusReporter;
    private final TableWriterFactory tableFactory;
    private final PrintStream out;

    private static final String STATE_PENDING = "PENDING";

    public StandardCompactionJobStatusReporter() {
        this(System.out);
    }

    public StandardCompactionJobStatusReporter(PrintStream out) {
        this.out = out;
        TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
        stateField = tableFactoryBuilder.addField("STATE");
        createTimeField = tableFactoryBuilder.addField("CREATE_TIME");
        jobIdField = tableFactoryBuilder.addField("JOB_ID");
        partitionIdField = tableFactoryBuilder.addField("PARTITION_ID");
        typeField = tableFactoryBuilder.addField("TYPE");
        standardProcessStatusReporter = new StandardProcessStatusReporter(out, tableFactoryBuilder);
        tableFactory = tableFactoryBuilder.build();
    }

    public void report(List<CompactionJobStatus> jobStatusList, JobQuery.Type queryType) {
        out.println();
        out.println("Compaction Job Status Report");
        out.println("----------------------------");
        printSummary(jobStatusList, queryType);
        if (!queryType.equals(JobQuery.Type.DETAILED)) {
            tableFactory.tableBuilder()
                    .showFields(queryType != JobQuery.Type.UNFINISHED, standardProcessStatusReporter.getFinishedFields())
                    .itemsAndSplittingWriter(jobStatusList, this::writeJob)
                    .build().write(out);
        }
    }

    private void printSummary(List<CompactionJobStatus> jobStatusList, JobQuery.Type queryType) {
        if (queryType.equals(JobQuery.Type.RANGE)) {
            printRangeSummary(jobStatusList);
        }
        if (queryType.equals(JobQuery.Type.DETAILED)) {
            printDetailedSummary(jobStatusList);
        }
        if (queryType.equals(JobQuery.Type.UNFINISHED)) {
            printUnfinishedSummary(jobStatusList);
        }
        if (queryType.equals(JobQuery.Type.ALL)) {
            printAllSummary(jobStatusList);
        }
    }

    private void printRangeSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total jobs in defined range: %d%n",
                jobStatusList.size());
        printAverageCompactionRate("Average compaction rate: %s%n", jobStatusList);
        printAverageCompactionRate("Average standard compaction rate: %s%n", standardJobs(jobStatusList));
        printAverageCompactionRate("Average splitting compaction rate: %s%n", splittingJobs(jobStatusList));
    }

    private void printDetailedSummary(List<CompactionJobStatus> jobStatusList) {
        if (jobStatusList.isEmpty()) {
            out.printf("No job found with provided jobId%n");
            out.printf("--------------------------%n");
        } else {
            jobStatusList.forEach(this::printSingleJobSummary);
        }
    }

    private void printSingleJobSummary(CompactionJobStatus jobStatus) {
        out.printf("Details for job %s:%n", jobStatus.getJobId());
        out.printf("State: %s%n", getState(jobStatus));
        out.printf("Creation Time: %s%n", jobStatus.getCreateUpdateTime().toString());
        out.printf("Partition ID: %s%n", jobStatus.getPartitionId());
        out.printf("Child partition IDs: %s%n", jobStatus.getChildPartitionIds().toString());
        jobStatus.getJobRuns().forEach(standardProcessStatusReporter::printProcessJobRun);
        out.println("--------------------------");
    }

    private void printUnfinishedSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total unfinished jobs: %d%n", jobStatusList.size());
        out.printf("Total unfinished jobs in progress: %d%n",
                jobStatusList.stream().filter(CompactionJobStatus::isStarted).count());
        out.printf("Total unfinished jobs not started: %d%n",
                jobStatusList.size() - jobStatusList.stream().filter(CompactionJobStatus::isStarted).count());
    }

    private void printAllSummary(List<CompactionJobStatus> jobStatusList) {
        List<CompactionJobStatus> splittingJobs = splittingJobs(jobStatusList);
        List<CompactionJobStatus> standardJobs = standardJobs(jobStatusList);
        out.printf("Total jobs: %d%n", jobStatusList.size());
        printAverageCompactionRate("Average compaction rate: %s%n", jobStatusList);
        out.println();
        out.printf("Total standard jobs: %d%n", standardJobs.size());
        out.printf("Total standard jobs pending: %d%n", standardJobs.stream().filter(job -> !job.isStarted()).count());
        out.printf("Total standard jobs in progress: %d%n", standardJobs.stream().filter(job -> job.isStarted() && !job.isFinished()).count());
        out.printf("Total standard jobs finished: %d%n", standardJobs.stream().filter(CompactionJobStatus::isFinished).count());
        printAverageCompactionRate("Average standard compaction rate: %s%n", standardJobs);
        out.println();
        out.printf("Total splitting jobs: %d%n", splittingJobs.size());
        out.printf("Total splitting jobs pending: %d%n", splittingJobs.stream().filter(job -> !job.isStarted()).count());
        out.printf("Total splitting jobs in progress: %d%n", splittingJobs.stream().filter(job -> job.isStarted() && !job.isFinished()).count());
        out.printf("Total splitting jobs finished: %d%n", splittingJobs.stream().filter(CompactionJobStatus::isFinished).count());
        printAverageCompactionRate("Average splitting compaction rate: %s%n", splittingJobs);
    }

    private static List<CompactionJobStatus> standardJobs(List<CompactionJobStatus> jobStatusList) {
        return jobStatusList.stream()
                .filter(job -> !job.isSplittingCompaction())
                .collect(Collectors.toList());
    }

    private static List<CompactionJobStatus> splittingJobs(List<CompactionJobStatus> jobStatusList) {
        return jobStatusList.stream()
                .filter(CompactionJobStatus::isSplittingCompaction)
                .collect(Collectors.toList());
    }

    private void printAverageCompactionRate(String formatString, List<CompactionJobStatus> jobs) {
        AverageRecordRate average = recordRate(jobs);
        if (average.getJobCount() < 1) {
            return;
        }
        String rateString = String.format("%s read/s, %s write/s",
                formatDecimal(average.getRecordsReadPerSecond()),
                formatDecimal(average.getRecordsWrittenPerSecond()));
        out.printf(formatString, rateString);
    }

    private static AverageRecordRate recordRate(List<CompactionJobStatus> jobs) {
        return AverageRecordRate.of(jobs.stream()
                .flatMap(job -> job.getJobRuns().stream())
                .filter(ProcessRun::isFinished)
                .map(ProcessRun::getFinishedSummary));
    }

    private void writeJob(CompactionJobStatus job, TableWriter.Builder table) {
        if (job.getJobRuns().isEmpty()) {
            table.row(row -> {
                row.value(stateField, STATE_PENDING);
                writeJobFields(job, row);
            });
        } else {
            job.getJobRuns().forEach(run -> table.row(row -> {
                writeJobFields(job, row);
                row.value(stateField, StandardProcessStatusReporter.getState(run));
                standardProcessStatusReporter.writeRunFields(run, row);
            }));
        }
    }

    private void writeJobFields(CompactionJobStatus job, TableRow.Builder builder) {
        builder.value(createTimeField, job.getCreateUpdateTime())
                .value(jobIdField, job.getJobId())
                .value(partitionIdField, job.getPartitionId())
                .value(typeField, job.isSplittingCompaction() ? "SPLIT" : "COMPACT");
    }

    private static String getState(CompactionJobStatus job) {
        if (job.isFinished()) {
            return STATE_FINISHED;
        } else if (job.isStarted()) {
            return STATE_IN_PROGRESS;
        }
        return STATE_PENDING;
    }
}
