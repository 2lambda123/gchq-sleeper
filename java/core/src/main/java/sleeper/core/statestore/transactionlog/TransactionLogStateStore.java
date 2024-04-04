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

import sleeper.core.schema.Schema;
import sleeper.core.statestore.DelegatingStateStore;
import sleeper.core.util.ExponentialBackoffWithJitter;

public class TransactionLogStateStore extends DelegatingStateStore {

    public TransactionLogStateStore(Builder builder) {
        super(
                new TransactionLogFileReferenceStore(
                        TransactionLogHead.forFiles(builder.filesLogStore, builder.maxAddTransactionAttempts, builder.retryBackoff)),
                new TransactionLogPartitionStore(builder.schema,
                        TransactionLogHead.forPartitions(builder.partitionsLogStore, builder.maxAddTransactionAttempts, builder.retryBackoff)));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Schema schema;
        private TransactionLogStore filesLogStore;
        private TransactionLogStore partitionsLogStore;
        private int maxAddTransactionAttempts = 10;
        private ExponentialBackoffWithJitter retryBackoff = new ExponentialBackoffWithJitter();

        private Builder() {
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder filesLogStore(TransactionLogStore filesLogStore) {
            this.filesLogStore = filesLogStore;
            return this;
        }

        public Builder partitionsLogStore(TransactionLogStore partitionsLogStore) {
            this.partitionsLogStore = partitionsLogStore;
            return this;
        }

        public Builder maxAddTransactionAttempts(int maxAddTransactionAttempts) {
            this.maxAddTransactionAttempts = maxAddTransactionAttempts;
            return this;
        }

        public Builder retryBackoff(ExponentialBackoffWithJitter retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public TransactionLogStateStore build() {
            return new TransactionLogStateStore(this);
        }
    }

}
