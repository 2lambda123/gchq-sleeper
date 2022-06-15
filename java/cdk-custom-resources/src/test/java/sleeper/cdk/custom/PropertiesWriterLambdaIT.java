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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TABLE_PROPERTIES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;

public class PropertiesWriterLambdaIT {

    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private AmazonS3 createClient() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private InstanceProperties createDefaultProperties(String account, String bucket) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, "id");
        instanceProperties.set(JARS_BUCKET, "myJars");
        instanceProperties.set(CONFIG_BUCKET, bucket);
        instanceProperties.set(REGION, "region");
        instanceProperties.set(VERSION, "1.2.3");
        instanceProperties.set(SUBNET, "subnet-12345");
        instanceProperties.set(VPC_ID, "vpc-12345");
        instanceProperties.set(ACCOUNT, account);
        instanceProperties.set(TABLE_PROPERTIES, "/path/to/table.properties");
        return instanceProperties;
    }

    @Test
    public void shouldUpdateS3BucketOnCreate() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        PropertiesWriterLambda propertiesWriterLambda = new PropertiesWriterLambda(client);

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(resourceProperties)
                .build();

        propertiesWriterLambda.handleEvent(event, null);

        // Then
        InstanceProperties loadedProperties = new InstanceProperties();
        loadedProperties.loadFromS3(client, bucketName);
        assertEquals("foo", loadedProperties.get(ACCOUNT));

        client.shutdown();

    }

    @Test
    public void shouldUpdateS3BucketOnUpdate() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        PropertiesWriterLambda propertiesWriterLambda = new PropertiesWriterLambda(client);

        client.putObject(bucketName, "config", "foo");

        // When
        InstanceProperties instanceProperties = createDefaultProperties("bar", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Update")
                .withResourceProperties(resourceProperties)
                .build();

        propertiesWriterLambda.handleEvent(event, null);

        // Then
        InstanceProperties loadedProperties = new InstanceProperties();
        loadedProperties.loadFromS3(client, bucketName);
        assertEquals("bar", loadedProperties.get(ACCOUNT));

        client.shutdown();
    }

    @Test
    public void shouldUpdateS3BucketAccordingToProperties() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        PropertiesWriterLambda propertiesWriterLambda = new PropertiesWriterLambda(client);
        String alternativeBucket = bucketName + "-alternative";

        client.createBucket(alternativeBucket);

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", alternativeBucket);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(resourceProperties)
                .build();

        propertiesWriterLambda.handleEvent(event, null);

        // Then
        InstanceProperties loadedProperties = new InstanceProperties();
        loadedProperties.loadFromS3(client, alternativeBucket);
        assertEquals("foo", loadedProperties.get(ACCOUNT));

        client.shutdown();
    }

    @Test
    public void shouldDeleteConfigObjectWhenCalledWithDeleteRequest() throws IOException {
        // Given
        AmazonS3 client = createClient();
        String bucketName = UUID.randomUUID().toString();
        client.createBucket(bucketName);
        client.putObject(bucketName, "config", "foo");

        // When
        InstanceProperties instanceProperties = createDefaultProperties("foo", bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("properties", instanceProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(resourceProperties)
                .build();

        PropertiesWriterLambda lambda = new PropertiesWriterLambda(client);
        lambda.handleEvent(event, null);

        // Then
        assertEquals(0, client.listObjects(bucketName).getObjectSummaries().size());
        client.shutdown();
    }
}
