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

package sleeper.ingest.status;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import sleeper.configuration.properties.InstanceProperties;

import java.util.Arrays;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.dynamodb.tools.DynamoDBUtils.initialiseTable;
import static sleeper.ingest.status.DynamoDBIngestJobStatusFormat.JOB_ID;
import static sleeper.ingest.status.DynamoDBIngestJobStatusFormat.UPDATE_TIME;
import static sleeper.ingest.status.DynamoDBIngestJobStatusStore.jobStatusTableName;

public class DynamoDBIngestJobStatusStoreCreator {
    private DynamoDBIngestJobStatusStoreCreator() {
    }

    public static void create(InstanceProperties properties, AmazonDynamoDB dynamoDB) {

        initialiseTable(dynamoDB, jobStatusTableName(properties.get(ID)),
                Arrays.asList(
                        new AttributeDefinition(JOB_ID, ScalarAttributeType.S),
                        new AttributeDefinition(UPDATE_TIME, ScalarAttributeType.N)),
                Arrays.asList(
                        new KeySchemaElement(JOB_ID, KeyType.HASH),
                        new KeySchemaElement(UPDATE_TIME, KeyType.RANGE)));

    }
}
