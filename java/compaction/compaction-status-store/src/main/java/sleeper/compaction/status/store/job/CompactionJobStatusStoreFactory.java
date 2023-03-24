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

package sleeper.compaction.status.store.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_STATUS_STORE_ENABLED;

public class CompactionJobStatusStoreFactory {
    private final AmazonDynamoDB dynamoDB;

    public CompactionJobStatusStoreFactory(AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
    }

    public static CompactionJobStatusStore getStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        if (properties.getBoolean(COMPACTION_STATUS_STORE_ENABLED)) {
            return new DynamoDBCompactionJobStatusStore(dynamoDB, properties);
        } else {
            return CompactionJobStatusStore.NONE;
        }
    }
}
