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
package sleeper.clients.deploy;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.regions.Region;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.instance.IngestProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
public class PopulatePropertiesIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.STS);
    private final AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                    localStackContainer.getEndpointOverride(LocalStackContainer.Service.STS).toString(),
                    localStackContainer.getRegion()))
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                    localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
            .build();

    @TempDir
    private Path tempDir;

    @Test
    void shouldPopulateInstancePropertiesCorrectly() {
        // Given/When
        InstanceProperties properties = populateInstancePropertiesBuilder()
                .sts(sts).regionProvider(() -> Region.of(localStackContainer.getRegion()))
                .build().populate();

        // Then
        InstanceProperties expected = new InstanceProperties();
        expected.setTags(Map.of("InstanceID", "test-instance"));
        expected.set(ID, "test-instance");
        expected.set(CONFIG_BUCKET, "sleeper-test-instance-config");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(QUERY_RESULTS_BUCKET, "sleeper-test-instance-query-results");
        expected.set(VPC_ID, "some-vpc");
        expected.set(SUBNETS, "some-subnet");
        expected.set(ECR_COMPACTION_REPO, "test-instance/compaction-job-execution");
        expected.set(ECR_INGEST_REPO, "test-instance/ingest");
        expected.set(BULK_IMPORT_REPO, "test-instance/bulk-import-runner");
        expected.set(ACCOUNT, sts.getCallerIdentity(new GetCallerIdentityRequest()).getAccount());
        expected.set(REGION, localStackContainer.getRegion());

        assertThat(properties).isEqualTo(expected);
    }

    @Test
    void shouldSaveBucketNamesToLocalDirectoryWhenInstancePropertiesGenerated() throws IOException {
        // Given
        InstanceProperties properties = populateInstancePropertiesBuilder()
                .sts(sts).regionProvider(() -> Region.of(localStackContainer.getRegion()))
                .build().populate();

        // When
        SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());

        // Then
        assertThat(tempDir.resolve("configBucket.txt")).exists();
        assertThat(tempDir.resolve("queryResultsBucket.txt")).exists();
    }

    @Test
    void shouldSaveBucketNamesToLocalDirectoryWhenTablePropertiesGenerated() throws IOException {
        // Given
        InstanceProperties instanceProperties = populateInstancePropertiesBuilder()
                .sts(sts).regionProvider(() -> Region.of(localStackContainer.getRegion()))
                .build().populate();
        TableProperties tableProperties = PopulateTableProperties.builder()
                .instanceProperties(instanceProperties)
                .tableProperties(new TableProperties(instanceProperties))
                .schema(schemaWithKey("key"))
                .tableName("test-table")
                .build().populate();

        // When
        SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.of(tableProperties));

        // Then
        assertThat(tempDir.resolve("tables/test-table/tableBucket.txt")).exists();
    }

    private PopulateInstanceProperties.Builder populateInstancePropertiesBuilder() {
        return PopulateInstanceProperties.builder()
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet");
    }
}
