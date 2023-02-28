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

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.InstanceProperties;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.GARBAGE_COLLECTOR_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;

/**
 * A {@link Stack} to garbage collect files which have been marked as being ready
 * for garbage collection after a compaction job.
 */
public class GarbageCollectorStack extends NestedStack {

    public GarbageCollectorStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            List<StateStoreStack> stateStoreStacks,
            List<IBucket> dataBuckets) {
        super(scope, id);

        // Config bucket
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));

        // Garbage collector code
        Code code = Code.fromBucket(jarsBucket, "lambda-garbagecollector-" + instanceProperties.get(VERSION) + ".jar");

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "garbage-collector"));

        // Garbage collector function
        Function handler = Function.Builder
                .create(this, "GarbageCollectorLambda")
                .functionName(functionName)
                .description("Scan DynamoDB looking for files that need deleting and delete them")
                .runtime(Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB))
                // Timeout is set to 90% of the period with which this runs to avoid 2 running simultaneously,
                // with a maximum of 900 seconds (15 minutes) which is the maximum execution time
                // of a lambda.
                .timeout(Duration.seconds(Math.max(1, Math.min((int) (0.9 * 60 * instanceProperties.getInt(GARBAGE_COLLECTOR_PERIOD_IN_MINUTES)), 900))))
                .code(code)
                .handler("sleeper.garbagecollector.GarbageCollectorLambda::eventHandler")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();

        // Grant this function permission delete files from the data bucket and
        // to read from / write to the DynamoDB table
        configBucket.grantRead(handler);
        dataBuckets.forEach(bucket -> bucket.grantRead(handler));
        dataBuckets.forEach(bucket -> bucket.grantDelete(handler));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWriteReadyForGCFileMetadata(handler));

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "GarbageCollectorPeriodicTrigger")
                .ruleName(instanceProperties.get(ID) + "-GarbageCollectorPeriodicTrigger")
                .description("A rule to periodically trigger the garbage collector")
                .enabled(Boolean.TRUE)
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(GARBAGE_COLLECTOR_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, rule.getRuleName());

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}
