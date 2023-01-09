/*
 * Copyright 2023 Crown Copyright
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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class StateMachineExecutorTest {
    private AWSStepFunctions stepFunctions;
    private TablePropertiesProvider tablePropertiesProvider;
    private AtomicReference<StartExecutionRequest> requested;
    private AmazonS3 amazonS3;

    @Before
    public void setUpStepFunctions() {
        requested = new AtomicReference<>();
        stepFunctions = mock(AWSStepFunctions.class);
        amazonS3 = mock(AmazonS3Client.class);
        tablePropertiesProvider = mock(TablePropertiesProvider.class);
        when(stepFunctions.startExecution(any(StartExecutionRequest.class)))
                .then((Answer<StartExecutionResult>) invocation -> {
                    requested.set(invocation.getArgument(0));
                    return null;
                });
        when(tablePropertiesProvider.getTableProperties(anyString()))
                .then((Answer<TableProperties>) x -> new TableProperties(new InstanceProperties()));
    }

    @Test
    public void shouldPassJobToStepFunctions() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.job")
                .isEqualTo(new Gson().toJson(myJob));
    }

    @Test
    public void shouldPassJobIdToSparkConfig() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.app.name="))
                .containsExactly("spark.app.name=my-job");
    }

    @Test
    public void shouldUseDefaultConfigurationIfNoneSpecified() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray()
                .contains("--conf");
    }

    @Test
    public void shouldThrowExceptionWhenInputFilesAreNull() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .build();

        // When / Then
        String expectedMessage = "The bulk import job failed validation with the following checks failing: \n"
                + "The input files must be set to a non-null and non-empty value.";
        assertThatThrownBy(() -> stateMachineExecutor.runJob(myJob))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    public void shouldOverwriteDefaultConfigurationIfSpecifiedInJob() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .sparkConf("spark.driver.memory", "10g")
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.driver.memory="))
                .containsExactly("spark.driver.memory=10g");
    }

    @Test
    public void shouldUseDefaultJobIdIfNoneWasPresentInTheJob() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.driver.memory="))
                .containsExactly("spark.driver.memory=7g");
    }

    @Test
    public void shouldSetJobIdToUUIDIfNotSetByUser() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .files(Lists.newArrayList("file1.parquet"))
                .tableName("myTable")
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.job.id").isString().isNotBlank();
    }

    @Test
    public void shouldPassConfigBucketToSparkArgs() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CONFIG_BUCKET, "myConfigBucket");
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .endsWith("myConfigBucket");
    }

    @Test
    public void shouldUseJobIdAsDriverPodName() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        StateMachineExecutor stateMachineExecutor = new StateMachineExecutor(stepFunctions, instanceProperties, tablePropertiesProvider, amazonS3);
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        stateMachineExecutor.runJob(myJob);

        // Then
        assertThatJson(requested.get().getInput())
                .inPath("$.args").isArray().extracting(Objects::toString)
                .filteredOn(s -> s.startsWith("spark.kubernetes.driver.pod.name="))
                .containsExactly("spark.kubernetes.driver.pod.name=my-job");
    }

}
