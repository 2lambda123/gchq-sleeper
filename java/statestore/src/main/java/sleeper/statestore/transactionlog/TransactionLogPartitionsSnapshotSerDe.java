/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.statestore.transactionlog;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.schema.Schema;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.statestore.StateStoreFileUtils;

import java.io.IOException;

public class TransactionLogPartitionsSnapshotSerDe {
    private final Schema sleeperSchema;
    private final StateStoreFileUtils stateStoreFileUtils;

    TransactionLogPartitionsSnapshotSerDe(Schema sleeperSchema, Configuration configuration) {
        this.sleeperSchema = sleeperSchema;
        this.stateStoreFileUtils = StateStoreFileUtils.forPartitions(configuration);
    }

    String save(String basePath, StateStorePartitions state, long lastTransactionNumber) throws IOException {
        String filePath = createPartitionsPath(basePath, lastTransactionNumber);
        stateStoreFileUtils.savePartitions(filePath, state, sleeperSchema);
        return filePath;
    }

    StateStorePartitions load(TransactionLogSnapshot snapshot) throws IOException {
        return load(snapshot.getPath());
    }

    StateStorePartitions load(String filePath) throws IOException {
        StateStorePartitions partitions = new StateStorePartitions();
        stateStoreFileUtils.loadPartitions(filePath, sleeperSchema, partitions::put);
        return partitions;
    }

    private String createPartitionsPath(String basePath, long lastTransactionNumber) {
        return basePath + "/snapshots/" + lastTransactionNumber + "-partitions.parquet";
    }

}
