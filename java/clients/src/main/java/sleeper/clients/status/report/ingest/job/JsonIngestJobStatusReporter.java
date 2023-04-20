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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.clients.status.report.job.JsonRecordsProcessedSummary;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.util.GsonConfig;

import java.io.PrintStream;
import java.util.List;

public class JsonIngestJobStatusReporter implements IngestJobStatusReporter {
    private final Gson gson = GsonConfig.standardBuilder()
            .registerTypeAdapter(RecordsProcessedSummary.class, JsonRecordsProcessedSummary.serializer())
            .registerTypeAdapter(IngestJobStatus.class, ingestJobStatusJsonSerializer())
            .create();
    private final PrintStream out;

    public JsonIngestJobStatusReporter() {
        this(System.out);
    }

    public JsonIngestJobStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(List<IngestJobStatus> statusList, JobQuery.Type queryType, IngestQueueMessages queueMessages) {
        out.println(gson.toJson(createJsonReport(statusList, queueMessages)));
    }

    private JsonObject createJsonReport(List<IngestJobStatus> statusList, IngestQueueMessages queueMessages) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("queueMessages", gson.toJsonTree(queueMessages));
        jsonObject.add("jobList", gson.toJsonTree(statusList));
        return jsonObject;
    }

    private static JsonSerializer<IngestJobStatus> ingestJobStatusJsonSerializer() {
        return (jobStatus, type, context) -> createIngestJobJson(jobStatus, context);
    }

    private static JsonElement createIngestJobJson(IngestJobStatus jobStatus, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("jobId", jobStatus.getJobId());
        jsonObject.add("jobRunList", context.serialize(jobStatus.getJobRuns()));
        return jsonObject;
    }
}
