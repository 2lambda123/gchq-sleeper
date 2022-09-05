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
package sleeper.core.iterator.impl;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * This is an example implementation of a {@link SortedRecordIterator}. It
 * filters out {@link Record}s based on a timestamp. If the timestamp is more
 * than a certain length of time ago then the record is removed.
 */
public class AgeOffIterator implements SortedRecordIterator {
    private String fieldName;
    private long ageOff;

    public AgeOffIterator() {
    }

    @Override
    public void init(String configString, Schema schema) {
        String[] fields = configString.split(",");
        if (2 != fields.length) {
            throw new IllegalArgumentException("Configuration string should have 2 fields: field name and age off time");
        }
        fieldName = fields[0];
        ageOff = Long.parseLong(fields[1]);
    }

    @Override
    public List<String> getRequiredValueFields() {
        return Collections.singletonList(fieldName);
    }

    @Override
    public CloseableIterator<Record> apply(CloseableIterator<Record> input) {
        return new AgeOffIteratorInternal(input, fieldName, ageOff);
    }

    public static class AgeOffIteratorInternal implements CloseableIterator<Record> {
        private final CloseableIterator<Record> input;
        private final String fieldName;
        private final long age;
        private Record next;

        public AgeOffIteratorInternal(
                CloseableIterator<Record> input,
                String fieldName,
                long age) {
            this.input = input;
            this.fieldName = fieldName;
            this.age = age;
            this.next = null;
            advance();
        }

        @Override
        public boolean hasNext() {
            return null != next;
        }

        @Override
        public Record next() {
            Record record = new Record(next);
            if (!input.hasNext()) {
                next = null;
            }
            advance();
            return record;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        private void advance() {
            while (input.hasNext()) {
                next = input.next();
                Long value = (Long) next.get(fieldName);
                if (null != value && System.currentTimeMillis() - value < age) {
                    break;
                } else {
                    next = null;
                }
            }
        }
    }
}
