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
package sleeper.core.range;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Serialises a {@link Region} to and from a JSON string.
 */
public class RegionSerDe {
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String MIN_INCLUSIVE = "minInclusive";
    public static final String MAX_INCLUSIVE = "maxInclusive";
    public static final String STRINGS_BASE64_ENCODED = "stringsBase64Encoded";

    private final Schema schema;
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public RegionSerDe(Schema schema) {
        try {
            this.schema = schema;
            GsonBuilder builder = new GsonBuilder()
                    .registerTypeAdapter(Class.forName(Region.class.getName()), new RegionJsonSerDe(this.schema))
                    .serializeNulls();
            this.gson = builder.create();
            this.gsonPrettyPrinting = builder.setPrettyPrinting().create();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Exception creating Gson", e);
        }
    }

    public String toJson(Region region) {
        return gson.toJson(region);
    }

    public String toJson(Region region, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(region);
        }
        return toJson(region);
    }

    public Region fromJson(String jsonSchema) {
        return gson.fromJson(jsonSchema, Region.class);
    }

    public static class RegionJsonSerDe implements JsonSerializer<Region>, JsonDeserializer<Region> {
        private final Schema schema;
        private final RangeFactory rangeFactory;

        public RegionJsonSerDe(Schema schema) {
            this.schema = schema;
            this.rangeFactory = new RangeFactory(schema);
        }

        @Override
        public JsonElement serialize(Region region, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context) {
            JsonObject json = new JsonObject();
            List<Range> ranges = region.getRanges();
            for (Range range : ranges) {
                json.add(range.getFieldName(), convertRangeToJsonObject(range));
            }
            json.addProperty(STRINGS_BASE64_ENCODED, Boolean.TRUE);
            return json;
        }

        @Override
        public Region deserialize(JsonElement jsonElement, java.lang.reflect.Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (!jsonElement.isJsonObject()) {
                throw new JsonParseException("Expected JsonObject, got " + jsonElement);
            }
            JsonObject jsonObject = jsonElement.getAsJsonObject();

            boolean stringsBase64Encoded = true;
            if (jsonObject.has(STRINGS_BASE64_ENCODED)) {
                stringsBase64Encoded = jsonObject.get(STRINGS_BASE64_ENCODED).getAsBoolean();
            }

            List<Range> ranges = new ArrayList<>();
            Set<Map.Entry<String, JsonElement>> entries = jsonObject.entrySet();
            for (Map.Entry<String, JsonElement> entry : entries) {
                String key = entry.getKey();
                if (key.equals(STRINGS_BASE64_ENCODED)) {
                    continue;
                }
                JsonElement json = entry.getValue();
                if (!json.isJsonObject()) {
                    throw new JsonParseException("Expected JsonObject, got " + json);
                }
                ranges.add(convertJsonObjectToRange(key, (JsonObject) json, stringsBase64Encoded));
            }
            return new Region(ranges);
        }

        private JsonObject convertRangeToJsonObject(Range range) {
            Optional<Type> optional = schema.getRowKeyFields()
                    .stream()
                    .filter(f -> f.getName().equals(range.getFieldName()))
                    .map(Field::getType)
                    .findFirst();
            if (!optional.isPresent()) {
                throw new JsonParseException("Cannot find type of field " + range.getFieldName() + " in schema");
            }
            PrimitiveType type = (PrimitiveType) optional.get();

            JsonObject json = new JsonObject();
            addObject(json, type, MIN, range.getMin());
            if (range.isMinInclusive()) {
                json.addProperty(MIN_INCLUSIVE, Boolean.TRUE);
            } else {
                json.addProperty(MIN_INCLUSIVE, Boolean.FALSE);
            }
            addObject(json, type, MAX, range.getMax());
            if (range.isMaxInclusive()) {
                json.addProperty(MAX_INCLUSIVE, Boolean.TRUE);
            } else {
                json.addProperty(MAX_INCLUSIVE, Boolean.FALSE);
            }

            return json;
        }

        private void addObject(JsonObject json, PrimitiveType type, String key, Object object) {
            if (null == object) {
                json.add(key, null);
                return;
            }
            if (type instanceof IntType) {
                json.addProperty(key, (Integer) object);
            } else if (type instanceof LongType) {
                json.addProperty(key, (Long) object);
            } else if (type instanceof StringType) {
                // Always serialise strings as base64
                byte[] stringAsBytes = ((String) object).getBytes(Charsets.UTF_8);
                String base64encodedBytes = Base64.getEncoder().encodeToString(stringAsBytes);
                json.addProperty(key, base64encodedBytes);
            } else if (type instanceof ByteArrayType) {
                byte[] bytes = (byte[]) object;
                String base64encodedBytes = Base64.getEncoder().encodeToString(bytes);
                json.addProperty(key, base64encodedBytes);
            } else {
                throw new JsonParseException("Unknown primitive type: " + type);
            }
        }

        private Range convertJsonObjectToRange(String fieldName, JsonObject json, boolean stringsBase64Encoded) {
            Object min = getObject(MIN, fieldName, json, stringsBase64Encoded);
            boolean minInclusive = json.has(MIN_INCLUSIVE) ?
                    json.get(MIN_INCLUSIVE).getAsBoolean() : true;
            Object max = getObject(MAX, fieldName, json, stringsBase64Encoded);
            boolean maxInclusive = json.has(MAX_INCLUSIVE) ?
                    json.get(MAX_INCLUSIVE).getAsBoolean() : false;
            return rangeFactory.createRange(schema.getField(fieldName).get(), min, minInclusive, max, maxInclusive);
        }

        private Object getObject(String key, String fieldName, JsonObject json, boolean stringsBase64Encoded) {
            if (!json.has(key)) {
                throw new JsonParseException("Missing " + key + " in " + json);
            }
            if (json.get(key).isJsonNull()) {
                return null;
            }
            Optional<Type> optional = schema.getRowKeyFields()
                    .stream()
                    .filter(f -> f.getName().equals(fieldName))
                    .map(Field::getType)
                    .findFirst();
            if (!optional.isPresent()) {
                throw new JsonParseException("Cannot find type of field " + fieldName + " in schema");
            }

            Object object;
            PrimitiveType type = (PrimitiveType) optional.get();
            if (type instanceof IntType) {
                object = json.get(key).getAsInt();
            } else if (type instanceof LongType) {
                object = json.get(key).getAsLong();
            } else if (type instanceof StringType) {
                if (stringsBase64Encoded) {
                    String encodedString = json.get(key).getAsString();
                    try {
                        byte[] stringAsBytes = Base64.getDecoder().decode(encodedString);
                        object = new String(stringAsBytes, Charsets.UTF_8);
                    } catch (IllegalArgumentException e) {
                        throw new JsonParseException("IllegalArgumentException base64 decoding the string " + encodedString, e);
                    }
                } else {
                    object = json.get(key).getAsString();
                }
            } else if (type instanceof ByteArrayType) {
                String encodedBytes = json.get(key).getAsString();
                object = Base64.getDecoder().decode(encodedBytes);
            } else {
                throw new JsonParseException("Unknown primitive type: " + type);
            }
            return object;
        }
    }
}
