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
package sleeper.core.statestore.inmemory;

import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.DelegatingStateStore;
import sleeper.core.statestore.StateStore;

import java.util.Arrays;
import java.util.List;

public class StateStoreTestHelper {

    private StateStoreTestHelper() {
    }

    public static StateStore inMemoryStateStoreWithFixedPartitions(Partition... partitions) {
        return inMemoryStateStoreWithFixedPartitions(Arrays.asList(partitions));
    }

    public static StateStore inMemoryStateStoreWithFixedPartitions(List<Partition> partitions) {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), new FixedPartitionStore(partitions));
    }

    public static StateStore inMemoryStateStoreWithFixedSinglePartition(Schema schema) {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), new FixedPartitionStore(schema));
    }

    public static StateStore inMemoryStateStoreWithSinglePartition(Schema schema) {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), InMemoryPartitionStore.withSinglePartition(schema));
    }

    public static StateStore inMemoryStateStoreWithPartitions(List<Partition> partitions) {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), new InMemoryPartitionStore(partitions));
    }

    public static StateStore inMemoryStateStoreWithNoPartitions() {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), new InMemoryPartitionStore(List.of()));
    }
}
