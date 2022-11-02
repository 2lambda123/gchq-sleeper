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
package sleeper.core.record;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

/**
 * Given a {@link Record}, creates a new {@link Record} containing all the fields
 * from the first record (more accurately all the fields from the {@link Schema}).
 */
public class CloneRecord {
    private final Schema schema;

    public CloneRecord(Schema schema) {
        this.schema = schema;
    }

    public Record clone(Record record) {
        Record clonedRecord = new Record();
        for (Field field : schema.getAllFields()) {
            clonedRecord.put(field.getName(), record.get(field.getName()));
        }
        return clonedRecord;
    }
}
