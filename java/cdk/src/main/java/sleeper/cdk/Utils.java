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
package sleeper.cdk;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.ecs.AwsLogDriverProps;
import software.amazon.awscdk.services.ecs.LogDriver;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.constructs.Construct;

import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.local.LoadLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.SleeperVersion;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.lang.String.format;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.configuration.properties.instance.CommonProperty.STACK_TAG_NAME;
import static sleeper.configuration.properties.instance.LoggingLevelsProperty.APACHE_LOGGING_LEVEL;
import static sleeper.configuration.properties.instance.LoggingLevelsProperty.AWS_LOGGING_LEVEL;
import static sleeper.configuration.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.configuration.properties.instance.LoggingLevelsProperty.PARQUET_LOGGING_LEVEL;
import static sleeper.configuration.properties.instance.LoggingLevelsProperty.ROOT_LOGGING_LEVEL;

/**
 * Collection of utility methods related to the CDK deployment
 */
public class Utils {

    /**
     * Region environment variable for setting in EC2 based ECS containers.
     */
    public static final String AWS_REGION = "AWS_REGION";

    private static final Pattern NUM_MATCH = Pattern.compile("^(\\d+)(\\D*)$");

    private Utils() {
        // Prevents instantiation
    }

    public static Map<String, String> createDefaultEnvironment(InstanceProperties instanceProperties) {
        Map<String, String> environmentVariables = new HashMap<>();
        environmentVariables.put(CONFIG_BUCKET.toEnvironmentVariable(),
                instanceProperties.get(CONFIG_BUCKET));

        environmentVariables.put("JAVA_TOOL_OPTIONS", createToolOptions(instanceProperties,
                LOGGING_LEVEL,
                ROOT_LOGGING_LEVEL,
                APACHE_LOGGING_LEVEL,
                PARQUET_LOGGING_LEVEL,
                AWS_LOGGING_LEVEL));

        return environmentVariables;
    }

    private static String createToolOptions(InstanceProperties instanceProperties, InstanceProperty... propertyNames) {
        StringBuilder sb = new StringBuilder();
        Arrays.stream(propertyNames)
                .filter(s -> instanceProperties.get(s) != null)
                .forEach(s -> sb.append("-D").append(s.getPropertyName())
                        .append("=").append(instanceProperties.get(s)).append(" "));

        return sb.toString();
    }

    public static String truncateToMaxSize(String input, int maxSize) {
        if (input.length() > maxSize) {
            return input.substring(0, maxSize);
        }
        return input;
    }

    public static String truncateTo64Characters(String input) {
        return truncateToMaxSize(input, 64);
    }

    /**
     * Valid values are taken from <a href="https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html">here</a>
     * A value of -1 represents an infinite number of days.
     *
     * @param numberOfDays number of days you want to retain the logs
     * @return The RetentionDays equivalent
     */
    public static RetentionDays getRetentionDays(int numberOfDays) {
        switch (numberOfDays) {
            case -1:
                return RetentionDays.INFINITE;
            case 1:
                return RetentionDays.ONE_DAY;
            case 3:
                return RetentionDays.THREE_DAYS;
            case 5:
                return RetentionDays.FIVE_DAYS;
            case 7:
                return RetentionDays.ONE_WEEK;
            case 14:
                return RetentionDays.TWO_WEEKS;
            case 30:
                return RetentionDays.ONE_MONTH;
            case 60:
                return RetentionDays.TWO_MONTHS;
            case 90:
                return RetentionDays.THREE_MONTHS;
            case 120:
                return RetentionDays.FOUR_MONTHS;
            case 150:
                return RetentionDays.FIVE_MONTHS;
            case 180:
                return RetentionDays.SIX_MONTHS;
            case 365:
                return RetentionDays.ONE_YEAR;
            case 400:
                return RetentionDays.THIRTEEN_MONTHS;
            case 545:
                return RetentionDays.EIGHTEEN_MONTHS;
            case 731:
                return RetentionDays.TWO_YEARS;
            case 1827:
                return RetentionDays.FIVE_YEARS;
            case 3653:
                return RetentionDays.TEN_YEARS;
            default:
                throw new IllegalArgumentException("Invalid number of days; see https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html for valid options");
        }
    }

    public static LogDriver createECSContainerLogDriver(Construct scope, InstanceProperties instanceProperties, String id) {
        AwsLogDriverProps logDriverProps = AwsLogDriverProps.builder()
                .streamPrefix(instanceProperties.get(ID) + "-" + id)
                .logGroup(LogGroup.Builder.create(scope, id)
                        .logGroupName(instanceProperties.get(ID) + "-" + id)
                        .retention(getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                        .build())
                .build();
        return LogDriver.awsLogs(logDriverProps);
    }

    public static <T extends InstanceProperties> T loadInstanceProperties(Function<Properties, T> properties, Construct scope) {
        return loadInstanceProperties(properties, tryGetContext(scope));
    }

    public static <T extends InstanceProperties> T loadInstanceProperties(
            Function<Properties, T> constructor, Function<String, String> tryGetContext) {
        Path propertiesFile = Path.of(tryGetContext.apply("propertiesfile"));
        T properties = LoadLocalProperties.loadInstanceProperties(constructor, propertiesFile);

        String validate = tryGetContext.apply("validate");
        String newinstance = tryGetContext.apply("newinstance");
        if (!"false".equalsIgnoreCase(validate)) {
            new ConfigValidator().validate(properties, propertiesFile);
        }
        if ("true".equalsIgnoreCase(newinstance)) {
            new NewInstanceValidator(AmazonS3ClientBuilder.defaultClient(),
                    AmazonDynamoDBClientBuilder.defaultClient()).validate(properties, propertiesFile);
        }
        String deployedVersion = properties.get(VERSION);
        String localVersion = SleeperVersion.getVersion();
        CdkDefinedInstanceProperty.getAll().forEach(properties::unset);

        if (!"true".equalsIgnoreCase(tryGetContext.apply("skipVersionCheck"))
                && deployedVersion != null
                && !localVersion.equals(deployedVersion)) {
            throw new MismatchedVersionException(format("Local version %s does not match deployed version %s. " +
                            "Please upgrade/downgrade to make these match",
                    localVersion, deployedVersion));
        }
        properties.set(VERSION, localVersion);
        return properties;
    }

    public static Stream<TableProperties> getAllTableProperties(
            InstanceProperties instanceProperties, Construct scope) {
        return getAllTableProperties(instanceProperties, tryGetContext(scope));
    }

    public static Stream<TableProperties> getAllTableProperties(
            InstanceProperties instanceProperties, Function<String, String> tryGetContext) {
        return LoadLocalProperties.loadTablesFromInstancePropertiesFile(
                instanceProperties, Path.of(tryGetContext.apply("propertiesfile")));
    }

    private static Function<String, String> tryGetContext(Construct scope) {
        return key -> (String) scope.getNode().tryGetContext(key);
    }

    public static boolean shouldDeployPaused(Construct scope) {
        return "true".equalsIgnoreCase((String) scope.getNode().tryGetContext("deployPaused"));
    }

    public static void addStackTagIfSet(Stack stack, InstanceProperties properties) {
        Optional.ofNullable(properties.get(STACK_TAG_NAME))
                .ifPresent(tagName -> Tags.of(stack).add(tagName, stack.getNode().getId()));
    }

    public static RemovalPolicy removalPolicy(InstanceProperties properties) {
        if (properties.getBoolean(RETAIN_INFRA_AFTER_DESTROY)) {
            return RemovalPolicy.RETAIN;
        } else {
            return RemovalPolicy.DESTROY;
        }
    }

    /**
     * Normalises EC2 instance size strings so they can be looked up in the
     * {@link software.amazon.awscdk.services.ec2.InstanceSize} enum.
     * Java identifiers can't start with a number, so "2xlarge" becomes "xlarge2".
     *
     * @param size the human readable size
     * @return the internal enum name
     */
    public static String normaliseSize(String size) {
        if (size == null) {
            return null;
        }
        Matcher sizeMatch = NUM_MATCH.matcher(size);
        if (sizeMatch.matches()) {
            // Match occurred so switch the capture groups
            return sizeMatch.group(2) + sizeMatch.group(1);
        } else {
            return size;
        }
    }

}
