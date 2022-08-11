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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.athena.TestUtils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public abstract class AbstractRecordHandlerIT {

    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    // For storing data
    @ClassRule
    public static TemporaryFolder tempDir = new TemporaryFolder();

    private static final Schema SCHEMA = new Schema();
    protected static final String SPILL_BUCKET_NAME = "spillbucket";
    protected static final String MIN_VALUE = Integer.toString(Integer.MIN_VALUE);

    private InstanceProperties instanceProperties;

    static {
        SCHEMA.setRowKeyFields(
                new Field("year", new IntType()),
                new Field("month", new IntType()),
                new Field("day", new IntType())
        );

        SCHEMA.setSortKeyFields(new Field("timestamp", new LongType()));

        SCHEMA.setValueFields(new Field("count", new LongType()),
                new Field("map", new MapType(new StringType(), new StringType())),
                new Field("str", new StringType()),
                new Field("list", new ListType(new StringType())));
    }

    @BeforeClass
    public static void createSpillBucket() {
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket(SPILL_BUCKET_NAME);
        s3Client.shutdown();
    }

    @Before
    public void createInstance() {
        this.instanceProperties = TestUtils.createInstance(createS3Client());
    }

    protected InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    protected void assertFieldContainedValue(Block records, int position, String fieldName, Object expectedValue) {
        FieldReader fieldReader = records.getFieldReader(fieldName);
        fieldReader.setPosition(position);

        Object value = fieldReader.readObject();
        assertThat(value).isEqualTo(expectedValue);
    }

    protected TableProperties createTable(InstanceProperties instanceProperties, Object... initialSplits) throws IOException {
        TableProperties table = createEmptyTable(instanceProperties, initialSplits);
        TestUtils.ingestData(createDynamoClient(), createS3Client(), tempDir.newFolder().getAbsolutePath(),
                instanceProperties, table);
        return table;
    }

    protected TableProperties createEmptyTable(InstanceProperties instanceProperties, Object... initialSplits) throws IOException {
        return TestUtils.createTable(instanceProperties, SCHEMA, tempDir.newFolder().getAbsolutePath(),
                createDynamoClient(), createS3Client(), initialSplits);
    }

    protected TableProperties createEmptyTable(InstanceProperties instanceProperties, Schema schema, Object... initialSplits) throws IOException {
        return TestUtils.createTable(instanceProperties, schema, tempDir.newFolder().getAbsolutePath(),
                createDynamoClient(), createS3Client(), initialSplits);
    }

    protected static AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    protected static AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    protected static org.apache.arrow.vector.types.pojo.Schema createArrowSchema() {
        return new SchemaBuilder()
                .addIntField("year")
                .addIntField("month")
                .addIntField("day")
                .addBigIntField("timestamp")
                .addBigIntField("count")
                .addStringField("str")
                .addListField("list", Types.MinorType.VARCHAR.getType())
                .build();
    }
}
