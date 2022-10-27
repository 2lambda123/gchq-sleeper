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

package sleeper.ingest;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.testutils.IngestRecordsTestDataHelper;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.UUID;

import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.schemaWithRowKeys;

public class IngestRecordsITBase {
    private static final int DYNAMO_PORT = 8000;

    @ClassRule
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    protected final Field field = new Field("key", new LongType());
    protected final Schema schema = schemaWithRowKeys(field);
    protected String inputFolderName;
    protected String sketchFolderName;

    @Before
    public void setUpBase() throws Exception {
        inputFolderName = folder.newFolder().getAbsolutePath();
        sketchFolderName = folder.newFolder().getAbsolutePath();
    }

    protected static DynamoDBStateStore getStateStore(Schema schema)
            throws StateStoreException {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
        String tableNameStub = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(tableNameStub, schema, dynamoDBClient);
        DynamoDBStateStore stateStore = dynamoDBStateStoreCreator.create();
        stateStore.initialise();
        return stateStore;
    }

    protected IngestProperties.Builder defaultPropertiesBuilder(StateStore stateStore, Schema sleeperSchema) {
        return IngestRecordsTestDataHelper.defaultPropertiesBuilder(stateStore, sleeperSchema, inputFolderName, sketchFolderName);
    }
}
