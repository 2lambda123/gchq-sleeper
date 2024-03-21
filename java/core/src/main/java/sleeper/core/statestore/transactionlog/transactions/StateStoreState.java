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
package sleeper.core.statestore.transactionlog.transactions;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.transactionlog.TransactionLogStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class StateStoreState {

    private final Map<String, Partition> partitionById = new HashMap<>();
    private final StateStoreFiles files = new StateStoreFiles();
    private long lastTransactionNumber = 0;

    public void update(TransactionLogStore logStore) {
        logStore.readTransactionsAfter(lastTransactionNumber).forEach(transaction -> {
            apply(transaction);
            lastTransactionNumber++;
        });
    }

    public Collection<Partition> partitions() {
        return partitionById.values();
    }

    public StateStoreFiles files() {
        return files;
    }

    private void apply(Object transaction) {
        if (transaction instanceof PartitionTransaction) {
            apply((PartitionTransaction) transaction);
        }
        if (transaction instanceof FileTransaction) {
            apply((FileTransaction) transaction);
        }
    }

    private void apply(PartitionTransaction transaction) {
        transaction.apply(partitionById);
    }

    private void apply(FileTransaction transaction) {
        transaction.apply(files);
    }

}
