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

import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.FunctionProps;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;

/**
 * The properties stack writes the Sleeper properties to S3 using a custom resource.
 */
public class PropertiesStack extends NestedStack {

    public PropertiesStack(Construct scope,
                           String id,
                           InstanceProperties instanceProperties) {
        super(scope, id);

        // Config bucket
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));

        HashMap<String, Object> properties = new HashMap<>();
        try {
            properties.put("properties", instanceProperties.saveAsString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialise properties");
        }

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "properties-writer"));

        Function propertiesWriterLambda = new Function(this, "PropertiesWriterLambda", FunctionProps.builder()
                .code(Code.fromBucket(jarsBucket, "cdk-custom-resources-" + instanceProperties.get(VERSION) + ".jar"))
                .functionName(functionName)
                .handler("sleeper.cdk.custom.PropertiesWriterLambda::handleEvent")
                .memorySize(2048)
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .description("Lambda for writing instance properties to S3 upon initialisation and teardown")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .runtime(Runtime.JAVA_11)
                .build());

        configBucket.grantWrite(propertiesWriterLambda);

        Provider propertiesWriterProvider = Provider.Builder.create(this, "PropertiesWriterProvider")
                .onEventHandler(propertiesWriterLambda)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();

        CustomResource.Builder.create(this, "InstanceProperties")
                .resourceType("Custom::InstanceProperties")
                .properties(properties)
                .serviceToken(propertiesWriterProvider.getServiceToken())
                .build();

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}
