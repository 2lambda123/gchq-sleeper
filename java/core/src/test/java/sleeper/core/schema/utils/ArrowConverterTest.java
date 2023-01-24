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

package sleeper.core.schema.utils;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.utils.ArrowConverter.convertSleeperPrimitiveFieldToArrowField;
import static sleeper.core.schema.utils.ArrowConverter.convertSleeperSchemaToArrowSchema;

public class ArrowConverterTest {
    private static final String FIELD_NAME = "test-field";

    @Test
    void shouldConvertSleeperPrimitiveFieldToArrowField() {
        // Given
        Field sleeperField = sleeperIntField(FIELD_NAME);

        // When
        org.apache.arrow.vector.types.pojo.Field arrowField = convertSleeperPrimitiveFieldToArrowField(sleeperField);

        // Then
        assertThat(arrowField)
                .isEqualTo(arrowPrimitiveField(FIELD_NAME, new ArrowType.Int(32, true)));
    }

    @Test
    void shouldConvertSleeperSchemaWithPrimitiveValueToArrowSchema() {
        // Given
        Schema sleeperSchema = Schema.builder()
                .rowKeyFields(sleeperStringField("key"))
                .valueFields(sleeperIntField("value"))
                .build();

        // When
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = convertSleeperSchemaToArrowSchema(sleeperSchema);

        // Then
        assertThat(arrowSchema.getFields())
                .containsExactly(
                        arrowPrimitiveField("key", new ArrowType.Utf8()),
                        arrowPrimitiveField("value", new ArrowType.Int(32, true)));
    }

    @Test
    void shouldConvertSleeperSchemaWithListValueToArrowSchema() {
        // Given
        Schema sleeperSchema = Schema.builder()
                .rowKeyFields(sleeperStringField("key"))
                .valueFields(sleeperListField("value", new IntType()))
                .build();

        // When
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = convertSleeperSchemaToArrowSchema(sleeperSchema);

        // Then
        assertThat(arrowSchema.getFields())
                .containsExactly(
                        arrowPrimitiveField("key", new ArrowType.Utf8()),
                        arrowListField("value", new ArrowType.Int(32, true)));
    }

    @Test
    void shouldConvertSleeperSchemaWithMapValueToArrowSchema() {
        // Given
        Schema sleeperSchema = Schema.builder()
                .rowKeyFields(sleeperStringField("key"))
                .valueFields(sleeperMapField("value", new StringType(), new IntType()))
                .build();

        // When
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = convertSleeperSchemaToArrowSchema(sleeperSchema);

        // Then
        assertThat(arrowSchema.getFields())
                .containsExactly(
                        arrowPrimitiveField("key", new ArrowType.Utf8()),
                        arrowMapField("value", new ArrowType.Utf8(), new ArrowType.Int(32, true)));
    }


    private static org.apache.arrow.vector.types.pojo.Field arrowMapField(String name, ArrowType keyType, ArrowType valueType) {
        return arrowListField(name,
                arrowStructField(name,
                        arrowPrimitiveField("key", keyType),
                        arrowPrimitiveField("value", valueType)));
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowListField(String name, ArrowType type) {
        return arrowListField(name, arrowPrimitiveField("element", type));
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowListField(String name, org.apache.arrow.vector.types.pojo.Field... field) {
        return new org.apache.arrow.vector.types.pojo.Field(name, FieldType.notNullable(new ArrowType.List()),
                List.of(field));
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowStructField(String name, org.apache.arrow.vector.types.pojo.Field... fields) {
        return new org.apache.arrow.vector.types.pojo.Field(
                name + "-key-value-struct",
                new org.apache.arrow.vector.types.pojo.FieldType(false, new ArrowType.Struct(), null),
                Stream.of(fields).collect(Collectors.toList()));
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowPrimitiveField(String name, ArrowType type) {
        return org.apache.arrow.vector.types.pojo.Field.notNullable(name, type);
    }

    private static Field sleeperMapField(String name, PrimitiveType keyType, PrimitiveType valueType) {
        return sleeperField(name, new MapType(keyType, valueType));
    }

    private static Field sleeperListField(String name, PrimitiveType type) {
        return sleeperField(name, new ListType(type));
    }

    private static Field sleeperStringField(String name) {
        return sleeperField(name, new StringType());
    }

    private static Field sleeperIntField(String name) {
        return sleeperField(name, new IntType());
    }

    private static Field sleeperField(String name, Type type) {
        return new Field(name, type);
    }
}
