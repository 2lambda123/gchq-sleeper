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
package sleeper.bulkimport.job.runner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.google.gson.JsonSyntaxException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;

/**
 * This abstract class executes a Spark job that reads in input Parquet files and writes
 * out files of {@link sleeper.core.record.Record}s. Concrete subclasses of this class must implement
 * a method which takes in a {@link Dataset} of {@link Row}s where a field has
 * been added that contains the Sleeper partition that the row is in and writes
 * the data to files in S3 and returns a list of the {@link FileInfo}s that
 * will then be used to update the {@link StateStore}.
 */
public class BulkImportJobDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobDriver.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final IngestJobStatusStore statusStore;
    private final Supplier<Instant> getTime;
    private final BulkImportJobSessionRunner jobSessionRunner;

    public BulkImportJobDriver(BulkImportJobRunner jobRunner, InstanceProperties instanceProperties,
                               AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        this(jobRunner, instanceProperties, s3Client, dynamoClient,
                IngestJobStatusStoreFactory.getStatusStore(dynamoClient, instanceProperties), Instant::now);
    }

    public BulkImportJobDriver(BulkImportJobRunner jobRunner, InstanceProperties instanceProperties,
                               AmazonS3 s3Client, AmazonDynamoDB dynamoClient,
                               IngestJobStatusStore statusStore,
                               Supplier<Instant> getTime) {
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        this.stateStoreProvider = new StateStoreProvider(dynamoClient, instanceProperties);
        this.statusStore = statusStore;
        this.getTime = getTime;
        this.jobSessionRunner = new BulkImportJobSparkSessionRunner(jobRunner, instanceProperties,
                tablePropertiesProvider, stateStoreProvider);
    }

    public void run(BulkImportJob job, String taskId) throws IOException {
        Instant startTime = getTime.get();
        LOGGER.info("Received bulk import job with id {} at time {}", job.getId(), startTime);
        LOGGER.info("Job is {}", job);
        statusStore.jobStarted(taskId, job.toIngestJob(), startTime);

        BulkImportJobOutput output = jobSessionRunner.run(job);

        long numRecords = output.numRecords();
        try {
            stateStoreProvider.getStateStore(job.getTableName(), tablePropertiesProvider)
                    .addFiles(output.fileInfos());
        } catch (StateStoreException e) {
            throw new RuntimeException("Failed to add files to state store. Ensure this service account has write access. Files may need to "
                    + "be re-imported for clients to accesss data");
        }
        LOGGER.info("Added {} files to statestore", output.numFiles());
        Instant finishTime = getTime.get();
        LOGGER.info("Finished bulk import job {} at time {}", job.getId(), finishTime);
        long durationInSeconds = Duration.between(startTime, finishTime).getSeconds();
        double rate = numRecords / (double) durationInSeconds;
        LOGGER.info("Bulk import job {} took {} seconds (rate of {} per second)", job.getId(), durationInSeconds, rate);
        statusStore.jobFinished(taskId, job.toIngestJob(), new RecordsProcessedSummary(
                new RecordsProcessed(numRecords, numRecords), startTime, finishTime));

        // Calling this manually stops it potentially timing out after 10 seconds.
        // Note that we stop the Spark context after we've applied the changes in Sleeper.
        output.stopSparkContext();
    }

    @FunctionalInterface
    public interface BulkImportJobSessionRunner {
        BulkImportJobOutput run(BulkImportJob job) throws IOException;
    }

    public static void start(String[] args, BulkImportJobRunner runner) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected 3 arguments: <config bucket name> <bulk import job ID> <bulk import task ID>");
        }
        String configBucket = args[0];
        String jobId = args[1];
        String taskId = args[2];

        InstanceProperties instanceProperties = new InstanceProperties();
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        try {
            instanceProperties.loadFromS3(amazonS3, configBucket);
        } catch (Exception e) {
            // This is a good indicator if something is wrong with the permissions
            LOGGER.error("Failed to load instance properties", e);
            LOGGER.info("Checking whether token is readable");
            String token = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
            java.nio.file.Path tokenPath = Paths.get(token);
            boolean readable = Files.isReadable(tokenPath);
            LOGGER.info("Token was{} readable", readable ? "" : " not");
            if (!readable) {
                PosixFileAttributes readAttributes = Files.readAttributes(tokenPath, PosixFileAttributes.class);
                LOGGER.info("Token Permissions: {}", readAttributes.permissions());
                LOGGER.info("Token owner: {}", readAttributes.owner());
            }
            // This could error if not logged in correctly
            AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
            GetCallerIdentityResult callerIdentity = sts.getCallerIdentity(new GetCallerIdentityRequest());
            LOGGER.info("Logged in as: {}", callerIdentity.getArn());

            throw e;
        }

        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        if (null == bulkImportBucket) {
            throw new RuntimeException("sleeper.bulk.import.bucket was not set. Has one of the bulk import stacks been deployed?");
        }
        String jsonJobKey = "bulk_import/" + jobId + ".json";
        LOGGER.info("Loading bulk import job from key {} in bulk import bucket {}", bulkImportBucket, jsonJobKey);
        String jsonJob = amazonS3.getObjectAsString(bulkImportBucket, jsonJobKey);
        BulkImportJob bulkImportJob;
        try {
            bulkImportJob = new BulkImportJobSerDe().fromJson(jsonJob);
        } catch (JsonSyntaxException e) {
            LOGGER.error("Json job was malformed: {}", jobId);
            throw e;
        }

        BulkImportJobDriver driver = new BulkImportJobDriver(runner, instanceProperties,
                amazonS3, AmazonDynamoDBClientBuilder.defaultClient());
        driver.run(bulkImportJob, taskId);
    }
}
