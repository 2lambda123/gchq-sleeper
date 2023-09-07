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

package sleeper.systemtest.suite.dsl.ingest;

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.datageneration.GenerateNumberedRecords;
import sleeper.systemtest.datageneration.GenerateNumberedValueOverrides;
import sleeper.systemtest.drivers.ingest.DirectIngestDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.List;
import java.util.stream.LongStream;

public class SystemTestDirectIngest {

    private final SleeperInstanceContext instance;
    private final DirectIngestDriver context;
    private final GenerateNumberedValueOverrides generatorOverrides;

    public SystemTestDirectIngest(SleeperInstanceContext instance, DirectIngestDriver context,
                                  GenerateNumberedValueOverrides generatorOverrides) {
        this.instance = instance;
        this.context = context;
        this.generatorOverrides = generatorOverrides;
    }

    public void numberedRecords(LongStream numbers) {
        context.ingest(GenerateNumberedRecords.from(getSchema(), generatorOverrides, numbers).iterator());
    }

    private Schema getSchema() {
        return instance.getTableProperties().getSchema();
    }

    public void records(Record... records) {
        context.ingest(List.of(records).iterator());
    }
}
