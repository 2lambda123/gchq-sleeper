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
package sleeper.systemtest.ingest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.text.RandomStringGenerator;
import sleeper.core.key.Key;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

/**
 * A {@link Supplier} of {@link Record}s generated randomly that conform to the
 * given schema.
 */
public class RandomRecordSupplier implements Supplier<Record> {
    public static final RandomRecordSupplierConfig DEFAULT_RANDOM_RECORD_SUPPLIER_CONFIG
            = new RandomRecordSupplierConfig(0, 100_000_000, 0, 10_000_000_000L, 10, 10, 10, 10);
    private final Map<String, Supplier<Object>> fieldNameToSupplier;

    public RandomRecordSupplier(Schema schema, RandomRecordSupplierConfig config) {
        fieldNameToSupplier = new HashMap<>();
        for (Field field : schema.getAllFields()) {
            fieldNameToSupplier.put(field.getName(), getSupplier(field.getType(), config));
        }
    }
    
    public RandomRecordSupplier(Schema schema) {
        this(schema, DEFAULT_RANDOM_RECORD_SUPPLIER_CONFIG);
    }

    @Override
    public Record get() {
        Record record = new Record();
        for (Map.Entry<String, Supplier<Object>> entry : fieldNameToSupplier.entrySet()) {
            record.put(entry.getKey(), entry.getValue().get());
        }
        return record;
    }

    public static Supplier<Object> getSupplier(Type type, RandomRecordSupplierConfig config) {
        if (type instanceof PrimitiveType) {
            return getSupplier((PrimitiveType) type, config);
        }
        if (type instanceof MapType) {
            return getSupplierForMapType((MapType) type, config);
        }
        if (type instanceof ListType) {
            return getSupplierForListType((ListType) type, config);
        }
        throw new IllegalArgumentException("Unknown type " + type);
    }

    public static Supplier<Object> getSupplier(PrimitiveType type, RandomRecordSupplierConfig config) {
        if (type instanceof IntType) {
            return new Supplier<Object>() {
                private final RandomDataGenerator generator = new RandomDataGenerator();

                @Override
                public Object get() {
                    return generator.nextInt(config.getMinRandomInt(), config.getMaxRandomInt());
                }
            };
        }
        if (type instanceof LongType) {
            return new Supplier<Object>() {
                private final RandomDataGenerator generator = new RandomDataGenerator();

                @Override
                public Object get() {
                    return generator.nextLong(config.getMinRandomLong(), config.getMaxRandomLong());
                }
            };
        }
        if (type instanceof StringType) {
            return new Supplier<Object>() {
                private final RandomStringGenerator generator = new RandomStringGenerator.Builder()
                        .withinRange('a', 'z')
                        .build();

                @Override
                public Object get() {
                    return generator.generate(config.getRandomStringLength());
                }
            };
        }
        if (type instanceof ByteArrayType) {
            return new Supplier<Object>() {
                private final Random random = new Random();

                @Override
                public Object get() {
                    byte[] bytes = new byte[config.getRandomByteArrayLength()];
                    random.nextBytes(bytes);
                    return bytes;
                }
            };
        }
        throw new IllegalArgumentException("Unknown type " + type);
    }

    public static Supplier<Object> getSupplierForMapType(MapType mapType, RandomRecordSupplierConfig config) {
        Supplier<Object> keySupplier = getSupplier(mapType.getKeyType(), config);
        Supplier<Object> valueSupplier = getSupplier(mapType.getValueType(), config);
        RandomDataGenerator generator = new RandomDataGenerator();
        int maxEntries = config.getMaxEntriesInRandomMap();
        return () -> {
            int numEntries = generator.nextInt(0, maxEntries);
            Map<Object, Object> map = new HashMap<>(numEntries);
            for (int i = 0; i < numEntries; i++) {
                map.put(keySupplier.get(), valueSupplier.get());
            }
            return map;
        };
    }

    public static Supplier<Object> getSupplierForListType(ListType listType, RandomRecordSupplierConfig config) {
        Supplier<Object> elementSupplier = getSupplier(listType.getElementType(), config);
        RandomDataGenerator generator = new RandomDataGenerator();
        int maxEntries = config.getMaxEntriesInRandomList();
        return () -> {
            int numEntries = generator.nextInt(0, maxEntries);
            List<Object> list = new ArrayList<>(numEntries);
            for (int i = 0; i < numEntries; i++) {
                list.add(elementSupplier.get());
            }
            return list;
        };
    }

    public static Supplier<Key> getSupplier(List<PrimitiveType> types, RandomRecordSupplierConfig config) {
        List<Supplier<Object>> suppliers = new ArrayList<>();
        for (PrimitiveType type : types) {
            suppliers.add(getSupplier(type, config));
        }
        return () -> {
            List<Object> result = new ArrayList<>();
            for (Supplier<Object> supplier : suppliers) {
                result.add(supplier.get());
            }
            return Key.create(result);
        };
    }
}
