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

import org.junit.Test;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SecurityFilteringIteratorTest {

    @Test
    public void shouldFilter() {
        // Given
        List<Record> records = getData();
        Iterator<Record> iterator = records.iterator();
        SecurityFilteringIterator securityFilteringIterator = new SecurityFilteringIterator();
        securityFilteringIterator.init("securityLabel,public", new Schema());

        // When
        Iterator<Record> filtered = securityFilteringIterator.apply(new WrappedIterator<>(iterator));

        // Then
        assertThat(filtered.hasNext()).isTrue();
        assertThat(filtered.next()).isEqualTo(records.get(0));
        assertThat(filtered.hasNext()).isTrue();
        assertThat(filtered.next()).isEqualTo(records.get(2));
        assertThat(filtered.hasNext()).isFalse();
    }

    @Test
    public void shouldAllowRecordsWithEmptyVisibilitiesEvenIfNoAuths() {
        // Given
        List<Record> records = getData();
        Iterator<Record> iterator = records.iterator();
        SecurityFilteringIterator securityFilteringIterator = new SecurityFilteringIterator();
        securityFilteringIterator.init("securityLabel", new Schema());

        // When
        Iterator<Record> filtered = securityFilteringIterator.apply(new WrappedIterator<>(iterator));

        // Then
        assertThat(filtered.hasNext()).isTrue();
        assertThat(filtered.next()).isEqualTo(records.get(2));
        assertThat(filtered.hasNext()).isFalse();
    }

    private static List<Record> getData() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("field1", "1");
        record1.put("field2", 1000L);
        record1.put("securityLabel", "public");
        records.add(record1);
        Record record2 = new Record();
        record2.put("field1", "2");
        record2.put("field2", 10000L);
        record2.put("securityLabel", "private");
        records.add(record2);
        Record record3 = new Record();
        record3.put("field1", "2");
        record3.put("field2", 100000L);
        record3.put("securityLabel", "");
        records.add(record3);
        return records;
    }
}
