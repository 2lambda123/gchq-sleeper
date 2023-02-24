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
package sleeper.cdk.stack.bulkimport;

import com.google.common.collect.Lists;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.CreateAlarmOptions;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.S3Code;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.util.Locale;
import java.util.Map;

import static sleeper.cdk.stack.IngestStack.addIngestSourceBucketReference;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;

public class CommonEmrBulkImportHelper {

    private final Construct scope;
    private final String shortId;
    private final InstanceProperties instanceProperties;

    public CommonEmrBulkImportHelper(Construct scope, String shortId, InstanceProperties instanceProperties) {
        this.scope = scope;
        this.shortId = shortId;
        this.instanceProperties = instanceProperties;
    }

    // Queue for messages to trigger jobs - note that each concrete substack
    // will have its own queue. The shortId is used to ensure the names of
    // the queues are different.
    public Queue createJobQueue(SystemDefinedInstanceProperty jobQueueUrl, ITopic errorsTopic) {
        String instanceId = instanceProperties.get(ID);
        Queue queueForDLs = Queue.Builder
                .create(scope, "BulkImport" + shortId + "JobDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "BulkImport" + shortId + "DLQ"))
                .build();

        DeadLetterQueue deadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queueForDLs)
                .build();

        queueForDLs.metricApproximateNumberOfMessagesVisible().with(MetricOptions.builder()
                        .period(Duration.seconds(60))
                        .statistic("Sum")
                        .build())
                .createAlarm(scope, "BulkImport" + shortId + "UndeliveredJobsAlarm", CreateAlarmOptions.builder()
                        .alarmDescription("Alarms if there are any messages that have failed validation or failed to start a " + shortId + " EMR Spark job")
                        .evaluationPeriods(1)
                        .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                        .threshold(0)
                        .datapointsToAlarm(1)
                        .treatMissingData(TreatMissingData.IGNORE)
                        .build())
                .addAlarmAction(new SnsAction(errorsTopic));

        Queue emrBulkImportJobQueue = Queue.Builder
                .create(scope, "BulkImport" + shortId + "JobQueue")
                .deadLetterQueue(deadLetterQueue)
                .queueName(instanceId + "-BulkImport" + shortId + "Q")
                .build();

        instanceProperties.set(jobQueueUrl, emrBulkImportJobQueue.getQueueUrl());

        return emrBulkImportJobQueue;
    }

    protected Function createJobStarterFunction(String bulkImportPlatform, Queue jobQueue,
                                                IBucket importBucket, CommonEmrBulkImportStack commonEmrStack) {
        String instanceId = instanceProperties.get(ID);
        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", bulkImportPlatform);
        S3Code code = Code.fromBucket(Bucket.fromBucketName(scope, "CodeBucketEMR", instanceProperties.get(JARS_BUCKET)),
                "bulk-import-starter-" + instanceProperties.get(UserDefinedInstanceProperty.VERSION) + ".jar");

        IBucket configBucket = Bucket.fromBucketName(scope, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceId.toLowerCase(Locale.ROOT), shortId, "bulk-import-job-starter"));

        Function function = Function.Builder.create(scope, "BulkImport" + shortId + "JobStarter")
                .code(code)
                .functionName(functionName)
                .description("Function to start " + shortId + " bulk import jobs")
                .memorySize(1024)
                .timeout(Duration.seconds(20))
                .environment(env)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .handler("sleeper.bulkimport.starter.BulkImportStarter")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .events(Lists.newArrayList(new SqsEventSource(jobQueue)))
                .build();

        configBucket.grantRead(function);
        importBucket.grantReadWrite(function);
        addIngestSourceBucketReference(scope, "IngestBucket", instanceProperties)
                .ifPresent(ingestBucket -> ingestBucket.grantRead(function));

        function.addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(Lists.newArrayList("iam:PassRole"))
                .resources(Lists.newArrayList(
                        commonEmrStack.getEmrRole().getRoleArn(),
                        commonEmrStack.getEc2Role().getRoleArn()
                ))
                .build());

        function.addToRolePolicy(PolicyStatement.Builder.create()
                .sid("CreateCleanupRole")
                .actions(Lists.newArrayList("iam:CreateServiceLinkedRole", "iam:PutRolePolicy"))
                .resources(Lists.newArrayList("arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*"))
                .conditions(Map.of("StringLike", Map.of("iam:AWSServiceName",
                        Lists.newArrayList("elasticmapreduce.amazonaws.com",
                                "elasticmapreduce.amazonaws.com.cn"))))
                .build());
        return function;
    }
}
