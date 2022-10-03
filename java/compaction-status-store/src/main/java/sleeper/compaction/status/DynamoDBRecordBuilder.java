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
package sleeper.compaction.status;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static sleeper.compaction.status.DynamoDBAttributes.createNumberAttribute;
import static sleeper.compaction.status.DynamoDBAttributes.createStringAttribute;

public class DynamoDBRecordBuilder {

    private final List<Attribute> attributes = new ArrayList<>();

    public DynamoDBRecordBuilder string(String key, String value) {
        return add(new Attribute(key, createStringAttribute(value)));
    }

    public DynamoDBRecordBuilder number(String key, Number value) {
        return add(new Attribute(key, createNumberAttribute(value)));
    }

    public DynamoDBRecordBuilder apply(Consumer<DynamoDBRecordBuilder> config) {
        config.accept(this);
        return this;
    }

    public Map<String, AttributeValue> build() {
        return attributes.stream()
                .collect(Collectors.toMap(Attribute::getKey, Attribute::getValue));
    }

    private DynamoDBRecordBuilder add(Attribute attribute) {
        attributes.add(attribute);
        return this;
    }

    private static class Attribute {
        private final String key;
        private final AttributeValue value;

        private Attribute(String key, AttributeValue value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public AttributeValue getValue() {
            return value;
        }
    }
}
