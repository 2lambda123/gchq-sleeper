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
package sleeper.core.statestore.transactionlog;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.PartitionStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transactions.StateStoreState;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

class TransactionLogPartitionStore implements PartitionStore {

    private final Schema schema;
    private final TransactionLogStore logStore;
    private final StateStoreState state;

    TransactionLogPartitionStore(Schema schema, TransactionLogStore logStore, StateStoreState state) {
        this.schema = schema;
        this.logStore = logStore;
        this.state = state;
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
    }

    @Override
    public void clearPartitionData() {
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        return partitions().collect(toUnmodifiableList());
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitions().filter(Partition::isLeafPartition).collect(toUnmodifiableList());
    }

    @Override
    public void initialise() throws StateStoreException {
        initialise(new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        logStore.addTransaction(new InitialisePartitionsTransaction(partitions));
    }

    private Stream<Partition> partitions() {
        state.update(logStore);
        return state.partitions().stream();
    }

}
