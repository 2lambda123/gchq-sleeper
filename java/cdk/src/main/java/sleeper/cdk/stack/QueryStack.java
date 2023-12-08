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
package sleeper.cdk.stack;

import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.actions.SQSActions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.ArnComponents;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.apigateway.IntegrationType;
import software.amazon.awscdk.services.apigatewayv2.CfnApi;
import software.amazon.awscdk.services.apigatewayv2.CfnIntegration;
import software.amazon.awscdk.services.apigatewayv2.CfnRoute;
import software.amazon.awscdk.services.apigatewayv2.alpha.WebSocketApi;
import software.amazon.awscdk.services.apigatewayv2.alpha.WebSocketApiAttributes;
import software.amazon.awscdk.services.apigatewayv2.alpha.WebSocketStage;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.ITable;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.Grant;
import software.amazon.awscdk.services.iam.GrantOnPrincipalOptions;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.Policy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.PolicyStatementProps;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Permission;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSourceProps;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.s3.LifecycleRule;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.IQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.instance.CommonProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS;

/**
 * A {@link NestedStack} to handle queries. This consists of a {@link Queue} that
 * queries are put on, a lambda {@link Function} to process them and another
 * {@link Queue} for the results to be posted to.
 */
@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class QueryStack extends NestedStack {
    public static final String QUERY_QUEUE_NAME = "QueryQueueName";
    public static final String QUERY_QUEUE_URL = "QueryQueueUrl";
    public static final String QUERY_DL_QUEUE_URL = "QueryDLQueueUrl";
    public static final String LEAF_PARTITION_QUERY_QUEUE_NAME = "LeafPartitionQueryQueueName";
    public static final String LEAF_PARTITION_QUERY_QUEUE_URL = "LeafPartitionQueryQueueUrl";
    public static final String LEAF_PARTITION_QUERY_DL_QUEUE_URL = "LeafPartitionQueryDLQueueUrl";
    public static final String QUERY_RESULTS_QUEUE_NAME = "QueryResultsQueueName";
    public static final String QUERY_RESULTS_QUEUE_URL = "QueryResultsQueueUrl";
    public static final String QUERY_LAMBDA_ROLE_ARN = "QueryLambdaRoleArn";

    private CfnApi webSocketApi;

    public QueryStack(Construct scope,
                      String id,
                      InstanceProperties instanceProperties,
                      BuiltJars jars,
                      CoreStacks coreStacks) {
        super(scope, id);

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());

        String tableName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "query-tracking-table"));

        // Query Tracking Table
        Table queryTrackingTable = Table.Builder.create(this, "QueryTrackingTable")
                .tableName(tableName)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .timeToLiveAttribute("expiryDate")
                .removalPolicy(RemovalPolicy.DESTROY)
                .partitionKey(Attribute.builder()
                        .name("queryId")
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name("subQueryId")
                        .type(AttributeType.STRING)
                        .build())
                .build();

        instanceProperties.set(QUERY_TRACKER_TABLE_NAME, queryTrackingTable.getTableName());

        // Queue for queries register
        Queue queryRegisterQueue = setupQueryRegisterQueue(instanceProperties);

        // Queue for leaf partition queries
        Queue leafPartitionQueriesQueue = setupLeafPartitionQueryQueues(instanceProperties);

        // Queue for results
        Queue queryResultsQueue = setupResultsQueue(instanceProperties);

        // Bucket for results
        IBucket queryResultsBucket = setupResultsBucket(instanceProperties);

        // Query register and execution lambda code
        LambdaCode queryJar = jars.lambdaCode(BuiltJar.QUERY, jarsBucket);

        String registerFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "query-register"));

        String leafQueryFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "query-executor"));

        // Lambda to register queries and post leaf partition queries to a queue
        IFunction queryRegisterLambda = createFunction("QueryRegisterLambda", queryJar, instanceProperties, registerFunctionName,
            "sleeper.query.lambda.SqsQueryRegisterLambda::handleRequest",
                "When a query arrives on the query SQS queue, this lambda is invoked to look for leaf partition queries");

        // Lambda to execute the queries
        IFunction leafPartitionQueryLambda = createFunction("QueryExecutorLambda", queryJar, instanceProperties, leafQueryFunctionName,
                "sleeper.query.lambda.SqsLeafPartitionQueryLambda::handleRequest",
                "When a query arrives on the query SQS queue, this lambda is invoked to execute the query");

        // Add the queue as a source of events for the lambdas
        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();

        queryRegisterLambda.addEventSource(new SqsEventSource(queryRegisterQueue, eventSourceProps));
        leafPartitionQueryLambda.addEventSource(new SqsEventSource(leafPartitionQueriesQueue, eventSourceProps));

        /* Grant the lambdas permission to read from the Dynamo tables, read from
           the S3 bucket, write back to the query queue and write to the results
           queue and S3 bucket */
        setPermissionsForLambda(coreStacks, jarsBucket, queryRegisterLambda, queryTrackingTable, queryRegisterQueue);
        setPermissionsForLambda(coreStacks, jarsBucket, leafPartitionQueryLambda, queryTrackingTable, queryRegisterQueue, queryResultsQueue, queryResultsBucket);

        /* Attach a policy to allow the lambda to put results in any S3 bucket or on any SQS queue.
           These policies look too open, but it's the only way to allow clients to be able to write to their
           buckets and queues.*/
        PolicyStatementProps policyStatementProps = PolicyStatementProps.builder()
                .effect(Effect.ALLOW)
                .actions(Arrays.asList(S3Actions.PutObject.getActionName(), SQSActions.SendMessage.getActionName()))
                .resources(Collections.singletonList("*"))
                .build();
        PolicyStatement policyStatement = new PolicyStatement(policyStatementProps);
        Policy policy = new Policy(this, "PutToAnyS3BucketAndSendToAnySQSPolicy");
        policy.addStatements(policyStatement);
        Objects.requireNonNull(queryRegisterLambda.getRole()).attachInlinePolicy(policy);
        Objects.requireNonNull(leafPartitionQueryLambda.getRole()).attachInlinePolicy(policy);

        /* Output the role of the lambda as a property so that clients that want the results of queries written
           to their own SQS queue can give the role permission to write to their queue */
        CfnOutputProps queryLambdaRoleOutputProps = new CfnOutputProps.Builder()
                .value(Objects.requireNonNull(queryRegisterLambda.getRole()).getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_LAMBDA_ROLE_ARN)
                .build();
        new CfnOutput(this, QUERY_LAMBDA_ROLE_ARN, queryLambdaRoleOutputProps);

        IRole role = Objects.requireNonNull(queryRegisterLambda.getRole());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_LAMBDA_ROLE, role.getRoleName());

        this.setupWebSocketApi(queryJar, instanceProperties, queryRegisterQueue, queryRegisterLambda, coreStacks);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private IFunction createFunction(String id, LambdaCode queryJar, InstanceProperties instanceProperties,
        String functionName, String handler, String description) {
        return queryJar.buildFunction(this, id, builder -> builder
                .functionName(functionName)
                .description(description)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .handler(handler)
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS))));
    }

    private Queue setupQueryRegisterQueue(InstanceProperties instanceProperties) {
        String dlQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueryDLQ");
        Queue queryRegisterQueueForDLs = Queue.Builder
                .create(this, "QueriesDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue queryRegisterDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queryRegisterQueueForDLs)
                .build();
        String queryRegisterQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueriesQueue");
        Queue queryRegisterQueue = Queue.Builder
                .create(this, "QueriesQueue")
                .queueName(queryRegisterQueueName)
                .deadLetterQueue(queryRegisterDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_QUEUE_URL, queryRegisterQueue.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_QUEUE_ARN, queryRegisterQueue.getQueueArn());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_DLQ_URL, queryRegisterQueueForDLs.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_DLQ_ARN, queryRegisterQueueForDLs.getQueueArn());

         CfnOutputProps queriesQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(queryRegisterQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_NAME)
                .build();
        new CfnOutput(this, QUERY_QUEUE_NAME, queriesQueueOutputNameProps);

        CfnOutputProps queriesQueueOutputProps = new CfnOutputProps.Builder()
                .value(queryRegisterQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_QUEUE_URL, queriesQueueOutputProps);

        CfnOutputProps queriesDLQueueOutputProps = new CfnOutputProps.Builder()
                .value(queryRegisterQueueForDLs.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_DL_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_DL_QUEUE_URL, queriesDLQueueOutputProps);

        return queryRegisterQueue;
    }

    private Queue setupLeafPartitionQueryQueues(InstanceProperties instanceProperties) {
        String dlLeafPartitionQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-LeafPartitionQueryDLQ");
        Queue leafPartitionQueryQueueForDLs = Queue.Builder
                .create(this, "LeafPartitionQueriesDeadLetterQueue")
                .queueName(dlLeafPartitionQueueName)
                .build();
        DeadLetterQueue leafPartitionQueriesDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(leafPartitionQueryQueueForDLs)
                .build();
        String leafPartitionQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-LeafPartitionQueriesQueue");
        Queue leafPartitionQueriesQueue = Queue.Builder
                .create(this, "LeafPartitionQueriesQueue")
                .queueName(leafPartitionQueueName)
                .deadLetterQueue(leafPartitionQueriesDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL, leafPartitionQueriesQueue.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_ARN, leafPartitionQueriesQueue.getQueueArn());
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_URL, leafPartitionQueryQueueForDLs.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_ARN, leafPartitionQueryQueueForDLs.getQueueArn());

        CfnOutputProps leafPartitionQueriesQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(leafPartitionQueriesQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_QUERY_QUEUE_NAME)
                .build();
        new CfnOutput(this, LEAF_PARTITION_QUERY_QUEUE_NAME, leafPartitionQueriesQueueOutputNameProps);

        CfnOutputProps leafPartitionQueriesQueueOutputProps = new CfnOutputProps.Builder()
                .value(leafPartitionQueriesQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_QUERY_QUEUE_URL)
                .build();
        new CfnOutput(this, LEAF_PARTITION_QUERY_QUEUE_URL, leafPartitionQueriesQueueOutputProps);

        CfnOutputProps leafPartitionQueriesDLQueueOutputProps = new CfnOutputProps.Builder()
                .value(leafPartitionQueryQueueForDLs.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + LEAF_PARTITION_QUERY_DL_QUEUE_URL)
                .build();
        new CfnOutput(this, LEAF_PARTITION_QUERY_DL_QUEUE_URL, leafPartitionQueriesDLQueueOutputProps);

        return leafPartitionQueriesQueue;
    }

    private Queue setupResultsQueue(InstanceProperties instanceProperties) {
        String queryResultsQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-QueryResultsQ");
        Queue queryResultsQueue = Queue.Builder
                .create(this, "QueryResultsQueue")
                .queueName(queryResultsQueueName)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL, queryResultsQueue.getQueueUrl());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_ARN, queryResultsQueue.getQueueArn());


        CfnOutputProps queryResultsQueueOutputNameProps = new CfnOutputProps.Builder()
                .value(queryResultsQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_RESULTS_QUEUE_NAME)
                .build();
        new CfnOutput(this, QUERY_RESULTS_QUEUE_NAME, queryResultsQueueOutputNameProps);

        CfnOutputProps queryResultsQueueOutputProps = new CfnOutputProps.Builder()
                .value(queryResultsQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + QUERY_RESULTS_QUEUE_URL)
                .build();
        new CfnOutput(this, QUERY_RESULTS_QUEUE_URL, queryResultsQueueOutputProps);

        return queryResultsQueue;
    }

    private IBucket setupResultsBucket(InstanceProperties instanceProperties) {
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);
        Bucket queryResultsBucket = Bucket.Builder
                .create(this, "QueryResultsBucket")
                .bucketName(String.join("-", "sleeper", instanceProperties.get(ID), "query-results").toLowerCase(Locale.ROOT))
                .versioned(false)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .removalPolicy(removalPolicy).autoDeleteObjects(removalPolicy == RemovalPolicy.DESTROY)
                .lifecycleRules(Collections.singletonList(
                        LifecycleRule.builder().expiration(Duration.days(instanceProperties.getInt(QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS))).build()))
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET, queryResultsBucket.getBucketName());

        return queryResultsBucket;
    }

    private void setPermissionsForLambda(CoreStacks coreStacks, IBucket jarsBucket, IFunction lambda,
            ITable queryTrackingTable, IQueue queryRegisterQueue) {
        coreStacks.grantReadTablesAndData(lambda);
        jarsBucket.grantRead(lambda);
        queryRegisterQueue.grantSendMessages(lambda);
        queryTrackingTable.grantReadWriteData(lambda);

    }

    private void setPermissionsForLambda(CoreStacks coreStacks, IBucket jarsBucket, IFunction lambda,
            ITable queryTrackingTable, IQueue queryRegisterQueue, IQueue queryResultsQueue, IBucket queryResultsBucket) {
        setPermissionsForLambda(coreStacks, jarsBucket, lambda, queryTrackingTable, queryRegisterQueue);
        queryResultsBucket.grantReadWrite(lambda);
        queryResultsQueue.grantSendMessages(lambda);
        queryResultsBucket.grantReadWrite(lambda);
        queryTrackingTable.grantReadWriteData(lambda);

    }
    protected void setupWebSocketApi(LambdaCode queryJar, InstanceProperties instanceProperties, Queue queriesQueue, IFunction queryExecutorLambda, CoreStacks coreStacks) {
        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        IFunction handler = queryJar.buildFunction(this, "apiHandler", builder -> builder
                .description("Prepares queries received via the WebSocket API and queues them for processing")
                .handler("sleeper.query.lambda.WebSocketQueryProcessorLambda::handleRequest")
                .environment(env)
                .memorySize(256)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .timeout(Duration.seconds(29))
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11));

        queriesQueue.grantSendMessages(handler);
        coreStacks.grantReadTablesConfig(handler);

        CfnApi api = CfnApi.Builder.create(this, "api")
                .name("sleeper-" + instanceProperties.get(ID) + "-query-api")
                .description("Sleeper Query API")
                .protocolType("WEBSOCKET")
                .routeSelectionExpression("$request.body.action")
                .build();
        this.webSocketApi = api;

        String integrationUri = Stack.of(this).formatArn(ArnComponents.builder()
                .service("apigateway")
                .account("lambda")
                .resource("path/2015-03-31/functions")
                .resourceName(handler.getFunctionArn() + "/invocations")
                .build());

        CfnIntegration integration = CfnIntegration.Builder.create(this, "integration")
                .apiId(api.getRef())
                .integrationType(IntegrationType.AWS_PROXY.name())
                .integrationUri(integrationUri)
                .build();

        // Note that we are deliberately using CFN L1 constructs to deploy the connect
        // route so that we are able to switch on AWS_IAM authentication. This is
        // currently not possible using the API Gateway L2 constructs
        CfnRoute.Builder.create(this, "connect-route")
                .apiId(api.getRef())
                .apiKeyRequired(false)
                .authorizationType("AWS_IAM")
                .routeKey("$connect")
                .target("integrations/" + integration.getRef())
                .build();

        CfnRoute.Builder.create(this, "default-route")
                .apiId(api.getRef())
                .apiKeyRequired(false)
                .routeKey("$default")
                .target("integrations/" + integration.getRef())
                .build();

        handler.addPermission("apigateway-access-to-lambda", Permission.builder()
                .principal(new ServicePrincipal("apigateway.amazonaws.com"))
                .sourceArn(Stack.of(this).formatArn(ArnComponents.builder()
                        .service("execute-api")
                        .resource(api.getRef())
                        .resourceName("*/*")
                        .build()))
                .build());

        WebSocketStage stage = WebSocketStage.Builder.create(this, "stage")
                .webSocketApi(WebSocketApi.fromWebSocketApiAttributes(this, "imported-api", WebSocketApiAttributes.builder()
                        .webSocketId(api.getRef())
                        .build()))
                .stageName("live")
                .autoDeploy(true)
                .build();
        stage.grantManagementApiAccess(handler);
        stage.grantManagementApiAccess(queryExecutorLambda);

        new CfnOutput(this, "WebSocketApiUrl", CfnOutputProps.builder()
                .value(stage.getUrl())
                .build());
        instanceProperties.set(CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL, stage.getUrl());
    }

    public Grant grantAccessToWebSocketQueryApi(IGrantable identity) {
        return Grant.addToPrincipal(GrantOnPrincipalOptions.builder()
                .grantee(identity)
                .actions(Collections.singletonList("execute-api:Invoke"))
                .resourceArns(Collections.singletonList(Stack.of(this).formatArn(ArnComponents.builder()
                        .service("execute-api")
                        .resource(this.webSocketApi.getRef())
                        .build())
                        + "/live/*"
                ))
                .build());
    }
}
