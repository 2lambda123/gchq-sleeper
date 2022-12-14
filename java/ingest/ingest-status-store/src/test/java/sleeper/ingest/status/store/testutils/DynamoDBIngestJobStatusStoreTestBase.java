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
package sleeper.ingest.status.store.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.After;
import org.junit.Before;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.dynamodb.tools.DynamoDBTestBase;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobTestData;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStoreCreator;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.ingest.status.store.testutils.IngestStatusStoreTestUtils.createInstanceProperties;
import static sleeper.ingest.status.store.testutils.IngestStatusStoreTestUtils.createSchema;
import static sleeper.ingest.status.store.testutils.IngestStatusStoreTestUtils.createTableProperties;

public class DynamoDBIngestJobStatusStoreTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate")
            .withIgnoredFieldsMatchingRegexes("jobRun.+updateTime").build();
    public static final String DEFAULT_TASK_ID = "task-id";
    public static final String DEFAULT_TASK_ID_2 = "task-id-2";
    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String jobStatusTableName = DynamoDBIngestJobStatusStore.jobStatusTableName(instanceProperties.get(ID));
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);

    protected final String tableName = tableProperties.get(TABLE_NAME);
    protected final IngestJobStatusStore store = DynamoDBIngestJobStatusStore.from(dynamoDBClient, instanceProperties);

    @Before
    public void setUp() {
        DynamoDBIngestJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @After
    public void tearDown() {
        dynamoDBClient.deleteTable(jobStatusTableName);
    }

    protected static RecordsProcessedSummary defaultSummary(Instant startTime, Instant finishTIme) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(200L, 100L),
                startTime, finishTIme);
    }

    protected IngestJobStatus getJobStatus(String jobId) {
        return store.getJob(jobId).orElse(null);
    }

    protected List<IngestJobStatus> getAllJobStatuses() {
        return store.getAllJobs(tableName);
    }

    protected IngestJob jobWithFiles(String... filenames) {
        return jobWithTableAndFiles(tableName, filenames);
    }

    protected IngestJob jobWithTableAndFiles(String tableName, String... filenames) {
        return IngestJobTestData.createJobWithTableAndFiles(UUID.randomUUID().toString(), tableName, filenames);
    }
}
