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

import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.statestore.StateStoreException;

import java.io.IOException;

/**
 * Writes a {@link Record} objects to the storage system, partitioned and sorted.
 * <p>
 * This class is an adaptor to {@link IngestCoordinator}.
 */
public class IngestRecords {

    private final IngestCoordinator<Record> ingestCoordinator;

    public IngestRecords(IngestCoordinator<Record> ingestCoordinator) {
        this.ingestCoordinator = ingestCoordinator;
    }

    public void init() throws StateStoreException {
        // Do nothing
    }

    public void write(Record record) throws IOException, IteratorException, StateStoreException {
        ingestCoordinator.write(record);
    }

    public IngestResult close() throws StateStoreException, IteratorException, IOException {
        return ingestCoordinator.closeReturningResult();
    }
}
