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

package sleeper.systemtest.suite.dsl.reports;

import sleeper.systemtest.drivers.compaction.CompactionReportsDriver;
import sleeper.systemtest.drivers.ingest.IngestReportsDriver;
import sleeper.systemtest.drivers.partitioning.PartitionReportDriver;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.reporting.ReportingContext;
import sleeper.systemtest.dsl.reporting.SystemTestReport;
import sleeper.systemtest.dsl.util.TestContext;

import java.util.ArrayList;
import java.util.List;

public class SystemTestReports {

    private final ReportingContext context;
    private final List<SystemTestReport> reports;

    private SystemTestReports(Builder builder) {
        context = builder.context;
        reports = builder.reports;
    }

    public static Builder builder(ReportingContext context) {
        return new Builder(context);
    }

    public static SystemTestBuilder builder(ReportingContext context,
                                            SleeperInstanceContext instance,
                                            SystemTestClients clients) {
        return new SystemTestBuilder(context, instance, clients);
    }

    public void print(TestContext testContext) {
        context.print(testContext, (out, startTime) ->
                reports.forEach(report -> report.print(out, startTime)));
    }

    public static class Builder {
        private final ReportingContext context;
        private final List<SystemTestReport> reports = new ArrayList<>();

        private Builder(ReportingContext context) {
            this.context = context;
        }

        public Builder report(SystemTestReport report) {
            reports.add(report);
            return this;
        }

        public SystemTestReports build() {
            return new SystemTestReports(this);
        }
    }

    public static class SystemTestBuilder extends Builder {

        private final SleeperInstanceContext instance;
        private final SystemTestClients clients;

        private SystemTestBuilder(ReportingContext context, SleeperInstanceContext instance, SystemTestClients clients) {
            super(context);
            this.instance = instance;
            this.clients = clients;
        }

        public Builder ingestTasksAndJobs() {
            return report(ingest().tasksAndJobsReport());
        }

        public Builder ingestJobs() {
            return report(ingest().jobsReport());
        }

        public Builder compactionTasksAndJobs() {
            return report(new CompactionReportsDriver(instance, clients.getDynamoDB())
                    .tasksAndJobsReport());
        }

        public Builder partitionStatus() {
            return report(new PartitionReportDriver(instance).statusReport());
        }

        private IngestReportsDriver ingest() {
            return new IngestReportsDriver(instance, clients.getDynamoDB(), clients.getSqs(), clients.getEmr());
        }
    }
}
