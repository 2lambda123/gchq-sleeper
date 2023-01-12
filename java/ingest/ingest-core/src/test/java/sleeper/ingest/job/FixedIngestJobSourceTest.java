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
package sleeper.ingest.job;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.IngestResultTestData.defaultFileIngestResult;

public class FixedIngestJobSourceTest {

    @Test
    public void shouldRetrieveSomeJobsAndMonitorResults() throws Exception {
        IngestJob job1 = IngestJob.builder()
                .id("test-job-1").tableName("test-table").files("test-file-1")
                .build();
        IngestJob job2 = IngestJob.builder()
                .id("test-job-2").tableName("test-table").files("test-file-2")
                .build();
        FixedIngestJobSource jobSource = FixedIngestJobSource.with(job1, job2);

        jobSource.consumeJobs(FixedIngestJobHandler.makingDefaultFiles());

        assertThat(jobSource.getIngestResults()).containsExactly(
                defaultFileIngestResult("test-file-1"),
                defaultFileIngestResult("test-file-2"));
    }

    @Test
    public void shouldRetrieveNoJobsAndMonitorResults() throws Exception {
        FixedIngestJobSource jobSource = FixedIngestJobSource.empty();

        jobSource.consumeJobs(FixedIngestJobHandler.makingDefaultFiles());

        assertThat(jobSource.getIngestResults()).isEmpty();
    }
}
