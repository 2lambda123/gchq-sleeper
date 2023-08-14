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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.google.gson.Gson;

import sleeper.bulkimport.configuration.ConfigurationUtils;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.validation.EmrInstanceArchitecture;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.instance.EKSProperty.EKS_IS_NATIVE_LIBS_IMAGE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.VERSION;

/**
 * A {@link StateMachinePlatformExecutor} Generates the arguments and configuration to
 * run a job using spark on an EKS cluster. It creates a list of arguments and
 * submits them to a state machine in AWS Step Functions.
 */
public class StateMachinePlatformExecutor implements PlatformExecutor {
    private static final String SPARK_IMAGE_JAR_LOCATION = "local:///opt/spark/work-dir/bulk-import-runner.jar";
    private static final String SPARK_IMAGE_JAVA_HOME = "/usr/local/openjdk-11";
    private static final String NATIVE_IMAGE_JAR_LOCATION = "local:///opt/spark/workdir/bulk-import-runner.jar";
    private static final String NATIVE_IMAGE_LOG4J_LOCATION = "file:///opt/spark/workdir/log4j.properties";
    private static final String NATIVE_IMAGE_JAVA_HOME = "/usr/lib/jvm/java-11-amazon-corretto";
    private static final Map<String, String> DEFAULT_CONFIG;

    private final AWSStepFunctions stepFunctions;
    private final InstanceProperties instanceProperties;

    static {
        Map<String, String> defaultConf = new HashMap<>();
        defaultConf.put("spark.executor.instances", "3");
        // Default Memory requests are overwritten because Fargate doesn't work with
        // Spark's default values
        defaultConf.put("spark.driver.memory", "7g");
        defaultConf.put("spark.executor.memory", "7g");
        // Fargate provides extra memory so no need to include extra which also messes
        // up the scheduler
        defaultConf.put("spark.driver.memoryOverhead", "1g");
        defaultConf.put("spark.executor.memoryOverhead", "1g");
        defaultConf.put("spark.kubernetes.authenticate.driver.serviceAccountName", "spark");
        // Hadoop Configuration
        defaultConf.put("spark.hadoop.fs.s3a.aws.credentials.provider",
                WebIdentityTokenCredentialsProvider.class.getName());
        defaultConf.put("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential");
        DEFAULT_CONFIG = Collections.unmodifiableMap(defaultConf);
    }

    public StateMachinePlatformExecutor(AWSStepFunctions stepFunctions,
                                        InstanceProperties instanceProperties) {
        this.stepFunctions = stepFunctions;
        this.instanceProperties = instanceProperties;
    }

    @Override
    public void runJobOnPlatform(BulkImportArguments arguments) {
        String stateMachineArn = instanceProperties.get(BULK_IMPORT_EKS_STATE_MACHINE_ARN);
        BulkImportJob bulkImportJob = arguments.getBulkImportJob();
        Map<String, Object> input = new HashMap<>();
        List<String> args = constructArgs(arguments, stateMachineArn);
        input.put("job", bulkImportJob);
        input.put("jobPodPrefix", jobPodPrefix(bulkImportJob));
        input.put("args", args);

        stepFunctions.startExecution(
                new StartExecutionRequest()
                        .withStateMachineArn(stateMachineArn)
                        .withName(String.join("-", bulkImportJob.getTableName(), bulkImportJob.getId()))
                        .withInput(new Gson().toJson(input)));
    }

    private Map<String, String> getDefaultSparkConfig(BulkImportJob bulkImportJob) {
        Map<String, String> defaultConfig = new HashMap<>(ConfigurationUtils.getSparkConfigurationFromInstanceProperties(instanceProperties, EmrInstanceArchitecture.X86_64));
        String imageName = instanceProperties.get(ACCOUNT) + ".dkr.ecr." +
                instanceProperties.get(REGION) + ".amazonaws.com/" +
                instanceProperties.get(BULK_IMPORT_REPO) + ":" + instanceProperties.get(VERSION);
        defaultConfig.put("spark.master", "k8s://" + instanceProperties.get(BULK_IMPORT_EKS_CLUSTER_ENDPOINT));
        defaultConfig.put("spark.app.name", bulkImportJob.getId());
        defaultConfig.put("spark.kubernetes.container.image", imageName);
        defaultConfig.put("spark.kubernetes.namespace", instanceProperties.get(BULK_IMPORT_EKS_NAMESPACE));
        String jobPodPrefix = jobPodPrefix(bulkImportJob);
        defaultConfig.put("spark.kubernetes.driver.pod.name", jobPodPrefix);
        defaultConfig.put("spark.kubernetes.executor.podNamePrefix", jobPodPrefix);

        defaultConfig.putAll(DEFAULT_CONFIG);

        return defaultConfig;
    }

    private List<String> constructArgs(BulkImportArguments arguments, String taskId) {
        BulkImportJob bulkImportJob = arguments.getBulkImportJob();
        Map<String, String> sparkProperties = getDefaultSparkConfig(bulkImportJob);

        // Create Spark conf by copying DEFAULT_CONFIG and over-writing any entries
        // which have been specified in the Spark conf on the bulk import job.
        if (null != bulkImportJob.getSparkConf()) {
            sparkProperties.putAll(bulkImportJob.getSparkConf());
        }

        // Point to locations in the Docker image
        String jarLocation;
        if (instanceProperties.getBoolean(EKS_IS_NATIVE_LIBS_IMAGE)) {
            sparkProperties.put("spark.executorEnv.JAVA_HOME", NATIVE_IMAGE_JAVA_HOME);
            sparkProperties.put("spark.driver.extraJavaOptions", "-Dlog4j.configuration=" + NATIVE_IMAGE_LOG4J_LOCATION);
            sparkProperties.put("spark.executor.extraJavaOptions", "-Dlog4j.configuration=" + NATIVE_IMAGE_LOG4J_LOCATION);
            jarLocation = NATIVE_IMAGE_JAR_LOCATION;
        } else {
            sparkProperties.put("spark.executorEnv.JAVA_HOME", SPARK_IMAGE_JAVA_HOME);
            jarLocation = SPARK_IMAGE_JAR_LOCATION;
        }

        BulkImportJob cloneWithUpdatedProps = new BulkImportJob.Builder()
                .className(bulkImportJob.getClassName())
                .files(bulkImportJob.getFiles())
                .id(bulkImportJob.getId())
                .tableName(bulkImportJob.getTableName())
                .platformSpec(bulkImportJob.getPlatformSpec())
                .sparkConf(sparkProperties)
                .build();
        return arguments.constructArgs(cloneWithUpdatedProps, taskId, jarLocation);
    }

    private static String jobPodPrefix(BulkImportJob job) {
        /* Spark adds extra IDs to the end of this - up to 17 characters, and performs some extra validation:
         * - whether the pod name prefix is <= 47 characters (https://spark.apache.org/docs/latest/running-on-kubernetes.html)
         * - whether the pod name prefix starts with a letter (https://kubernetes.io/docs/concepts/overview/working-with-objects/names/)
         * After adding a "job-" prefix, maximum id length = 47-(17+4) = 26 characters
         */
        if (job.getId().length() > 26) {
            return "job-" + job.getId().substring(0, 26);
        } else {
            return "job-" + job.getId();
        }
    }
}
