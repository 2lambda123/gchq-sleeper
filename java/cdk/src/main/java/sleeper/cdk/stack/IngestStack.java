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
package sleeper.cdk.stack;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import sleeper.cdk.Utils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.Alarm;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_DLQ_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_TASK_DEFINITION_FAMILY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_TASK_CPU;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_TASK_MEMORY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class IngestStack extends NestedStack {
    public static final String INGEST_STACK_QUEUE_NAME = "IngestStackQueueNameKey";
    public static final String INGEST_STACK_QUEUE_URL = "IngestStackQueueUrlKey";
    public static final String INGEST_STACK_DL_QUEUE_URL = "IngestStackDLQueueUrlKey";
    public static final String INGEST_CLUSTER_NAME = "IngestClusterName";
    public static final String INGEST_CONTAINER_ROLE_ARN = "IngestContainerRoleARN";

    private Queue ingestJobQueue;
    private Queue ingestDLQ;
    private final InstanceProperties instanceProperties;
    private final IngestStatusStoreStack statusStore;

    public IngestStack(
            Construct scope,
            String id,
            List<StateStoreStack> stateStoreStacks,
            List<IBucket> dataBuckets,
            Topic topic,
            InstanceProperties instanceProperties) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        this.statusStore = IngestStatusStoreStack.from(scope, instanceProperties);
        // The ingest stack consists of the following components:
        //  - An SQS queue for the ingest jobs.
        //  - An ECS cluster, task definition, etc., for ingest jobs.
        //  - A lambda that periodically checks the number of running ingest tasks
        //      and if there are not enough (i.e. there is a backlog on the queue
        //      then it creates more tasks).

        // Config bucket
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));

        // SQS queue for ingest jobs
        sqsQueueForIngestJobs(topic);

        // ECS cluster for ingest tasks
        ecsClusterForIngestTasks(configBucket, jarsBucket, dataBuckets, stateStoreStacks, ingestJobQueue);

        // Lambda to create ingest tasks
        lambdaToCreateIngestTasks(configBucket, ingestJobQueue);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private Queue sqsQueueForIngestJobs(Topic topic) {
        // Create queue for ingest job definitions
        String dlQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-IngestJobDLQ");

        ingestDLQ = Queue.Builder
                .create(this, "IngestJobDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue ingestJobDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(ingestDLQ)
                .build();
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-IngestJobQ");
        ingestJobQueue = Queue.Builder
                .create(this, "IngestJobQueue")
                .queueName(queueName)
                .deadLetterQueue(ingestJobDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, ingestJobQueue.getQueueUrl());
        instanceProperties.set(INGEST_JOB_DLQ_URL, ingestJobDeadLetterQueue.getQueue().getQueueUrl());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        Alarm ingestAlarm = Alarm.Builder
                .create(this, "IngestAlarm")
                .alarmDescription("Alarms if there are any messages on the dead letter queue for the ingest queue")
                .metric(ingestDLQ.metricApproximateNumberOfMessagesVisible()
                        .with(MetricOptions.builder().statistic("Sum").period(Duration.seconds(60)).build())
                )
                .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                .threshold(0)
                .evaluationPeriods(1)
                .datapointsToAlarm(1)
                .treatMissingData(TreatMissingData.IGNORE)
                .build();
        ingestAlarm.addAlarmAction(new SnsAction(topic));

        CfnOutputProps ingestJobQueueProps = new CfnOutputProps.Builder()
                .value(ingestJobQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + INGEST_STACK_QUEUE_URL)
                .build();
        new CfnOutput(this, INGEST_STACK_QUEUE_URL, ingestJobQueueProps);

        CfnOutputProps ingestJobQueueNameProps = new CfnOutputProps.Builder()
                .value(ingestJobQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + INGEST_STACK_QUEUE_NAME)
                .build();
        new CfnOutput(this, INGEST_STACK_QUEUE_NAME, ingestJobQueueNameProps);

        CfnOutputProps ingestJobDefinitionsDLQueueProps = new CfnOutputProps.Builder()
                .value(ingestJobDeadLetterQueue.getQueue().getQueueUrl())
                .build();
        new CfnOutput(this, INGEST_STACK_DL_QUEUE_URL, ingestJobDefinitionsDLQueueProps);

        return ingestJobQueue;
    }

    private Cluster ecsClusterForIngestTasks(
            IBucket configBucket,
            IBucket jarsBucket,
            List<IBucket> dataBuckets,
            List<StateStoreStack> stateStoreStacks,
            Queue ingestJobQueue) {
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(this, "VPC1", vpcLookupOptions);
        String clusterName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "ingest-cluster"));
        Cluster cluster = Cluster.Builder
                .create(this, "IngestCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        instanceProperties.set(INGEST_CLUSTER, cluster.getClusterName());

        FargateTaskDefinition taskDefinition = FargateTaskDefinition.Builder
                .create(this, "IngestTaskDefinition")
                .family(instanceProperties.get(ID) + "IngestTaskFamily")
                .cpu(instanceProperties.getInt(INGEST_TASK_CPU))
                .memoryLimitMiB(instanceProperties.getInt(INGEST_TASK_MEMORY))
                .build();
        instanceProperties.set(INGEST_TASK_DEFINITION_FAMILY, taskDefinition.getFamily());

        IRepository repository = Repository.fromRepositoryName(this,
                "ECR-ingest",
                instanceProperties.get(ECR_INGEST_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        ContainerDefinitionOptions containerDefinitionOptions = ContainerDefinitionOptions.builder()
                .image(containerImage)
                .logging(Utils.createFargateContainerLogDriver(this, instanceProperties, "IngestTasks"))
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .build();
        taskDefinition.addContainer("IngestContainer", containerDefinitionOptions);

        configBucket.grantRead(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());
        dataBuckets.forEach(bucket -> bucket.grantReadWrite(taskDefinition.getTaskRole()));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWriteActiveFileMetadata(taskDefinition.getTaskRole()));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadPartitionMetadata(taskDefinition.getTaskRole()));
        statusStore.grantWriteJobEvent(taskDefinition.getTaskRole());
        statusStore.grantWriteTaskEvent(taskDefinition.getTaskRole());
        ingestJobQueue.grantConsumeMessages(taskDefinition.getTaskRole());
        taskDefinition.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(Collections.singletonList("cloudwatch:PutMetricData"))
                .resources(Collections.singletonList("*"))
                .conditions(Collections.singletonMap("StringEquals", Collections.singletonMap("cloudwatch:namespace", instanceProperties.get(UserDefinedInstanceProperty.METRICS_NAMESPACE))))
                .build());

        // If a source bucket for ingest was specified, grant read access to it.
        String sourceBucketName = instanceProperties.get(UserDefinedInstanceProperty.INGEST_SOURCE_BUCKET);
        if (null != sourceBucketName && !sourceBucketName.isEmpty()) {
            IBucket sourceBucket = Bucket.fromBucketName(this, "SourceBucket", sourceBucketName);
            sourceBucket.grantRead(taskDefinition.getTaskRole());
        }

        CfnOutputProps ingestClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, INGEST_CLUSTER_NAME, ingestClusterProps);

        CfnOutputProps ingestRoleARNProps = new CfnOutputProps.Builder()
                .value(taskDefinition.getTaskRole().getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + INGEST_CONTAINER_ROLE_ARN)
                .build();
        new CfnOutput(this, INGEST_CONTAINER_ROLE_ARN, ingestRoleARNProps);

        return cluster;
    }

    private void lambdaToCreateIngestTasks(IBucket configBucket, Queue ingestJobQueue) {
        // Job creation code
        IBucket jarsBucket = Bucket.fromBucketArn(this,
                "jarsBucket-ingest",
                "arn:aws:s3:::" + instanceProperties.get(JARS_BUCKET));
        Code code = Code.fromBucket(jarsBucket, "ingest-" + instanceProperties.get(VERSION) + ".jar");

        // Run tasks function
        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "ingest-tasks-creator"));

        Function handler = Function.Builder
                .create(this, "IngestTasksCreator")
                .functionName(functionName)
                .description("If there are ingest jobs on queue create tasks to run them")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .code(code)
                .handler("sleeper.ingest.job.RunTasksLambda::eventHandler")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();

        // Grant this function permission to read from the S3 bucket
        configBucket.grantRead(handler);

        // Grant this function permission to query the queue for number of messages
        ingestJobQueue.grantSendMessages(handler);
        ingestJobQueue.grant(handler, "sqs:GetQueueAttributes");
        statusStore.grantWriteJobEvent(handler);
        statusStore.grantWriteTaskEvent(handler);
        // Grant this function permission to query ECS for the number of tasks, etc
        PolicyStatement policyStatement = PolicyStatement.Builder
                .create()
                .resources(Collections.singletonList("*"))
                .actions(Arrays.asList("ecs:ListTasks", "ecs:RunTask", "iam:PassRole"))
                .build();
        IRole role = Objects.requireNonNull(handler.getRole());
        role.addToPrincipalPolicy(policyStatement);
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        // Cloudwatch rule to trigger this lambda
        String ruleName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-IngestTasksCreationRule");
        Rule rule = Rule.Builder
                .create(this, "IngestTasksCreationPeriodicTrigger")
                .ruleName(ruleName)
                .description("A rule to periodically trigger the ingest tasks lambda")
                .enabled(Boolean.TRUE)
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(INGEST_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(INGEST_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(INGEST_CLOUDWATCH_RULE, rule.getRuleName());
    }

    public Queue getIngestJobQueue() {
        return ingestJobQueue;
    }

    public Queue getErrorQueue() {
        return ingestDLQ;
    }
}
