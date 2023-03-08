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

package sleeper.clients.admin;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import sleeper.clients.deploy.GenerateInstanceProperties;
import sleeper.clients.deploy.GenerateTableProperties;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.table.TableProperties;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class PropertiesDiffTest {

    @DisplayName("Compare instance properties")
    @Nested
    class CompareInstanceProperties {

        @Test
        void shouldDetectNoChanges() {
            // Given
            InstanceProperties before = createInstanceProperties();
            InstanceProperties after = createInstanceProperties();

            // When / Then
            assertThat(getChanges(before, after)).isEmpty();
        }

        @Test
        void shouldDetectPropertyHasBeenUpdated() {
            // Given
            InstanceProperties before = createInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "30");
            InstanceProperties after = createInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "50");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueChanged(MAXIMUM_CONNECTIONS_TO_S3, "30", "50"));
        }

        @Test
        void shouldDetectPropertyIsNewlySet() {
            // Given
            InstanceProperties before = createInstanceProperties();
            InstanceProperties after = createInstanceProperties();
            after.set(INGEST_SOURCE_BUCKET, "some-bucket");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(newValue(INGEST_SOURCE_BUCKET, "some-bucket"));
        }

        @Test
        void shouldDetectPropertyIsUnset() {
            // Given
            InstanceProperties before = createInstanceProperties();
            before.set(INGEST_SOURCE_BUCKET, "some-bucket");
            InstanceProperties after = createInstanceProperties();

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueDeleted(INGEST_SOURCE_BUCKET, "some-bucket"));
        }

        @Test
        void shouldDetectDefaultedPropertyIsNewlySet() {
            // Given
            InstanceProperties before = createInstanceProperties();
            InstanceProperties after = createInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "50");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(newValue(MAXIMUM_CONNECTIONS_TO_S3, "50"));
        }

        @Test
        void shouldDetectDefaultedPropertyIsUnset() {
            // Given
            InstanceProperties before = createInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "50");
            InstanceProperties after = createInstanceProperties();

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueDeleted(MAXIMUM_CONNECTIONS_TO_S3, "50"));
        }
    }

    @DisplayName("Compare unknown properties")
    @Nested
    class CompareUnknownProperties {
        @Test
        void shouldDetectNoChanges() throws Exception {
            // Given
            InstanceProperties before = new InstanceProperties(
                    loadProperties("unknown.property=1"));
            InstanceProperties after = new InstanceProperties(
                    loadProperties("unknown.property=1"));

            // When / Then
            assertThat(getChanges(before, after)).isEmpty();
        }

        @Test
        void shouldDetectPropertyHasBeenUpdated() throws Exception {
            // Given
            InstanceProperties before = new InstanceProperties(
                    loadProperties("unknown.property=1"));
            InstanceProperties after = new InstanceProperties(
                    loadProperties("unknown.property=2"));

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueChanged("unknown.property", "1", "2"));
        }

        @Test
        void shouldDetectPropertyIsNewlySet() throws Exception {
            // Given
            InstanceProperties before = new InstanceProperties();
            InstanceProperties after = new InstanceProperties(
                    loadProperties("unknown.property=12"));

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(newValue("unknown.property", "12"));
        }

        @Test
        void shouldDetectPropertyIsUnset() throws Exception {
            // Given
            InstanceProperties before = new InstanceProperties(
                    loadProperties("unknown.property=12"));
            InstanceProperties after = new InstanceProperties();

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueDeleted("unknown.property", "12"));
        }
    }

    @DisplayName("Compare table properties")
    @Nested
    class CompareTableProperties {

        @Test
        void shouldDetectPropertyHasBeenUpdated() {
            // Given
            TableProperties before = createTableProperties();
            before.set(ITERATOR_CONFIG, "config-before");
            TableProperties after = createTableProperties();
            after.set(ITERATOR_CONFIG, "config-after");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueChanged(ITERATOR_CONFIG, "config-before", "config-after"));
        }
    }

    private <T extends SleeperProperty> List<PropertyDiff> getChanges(SleeperProperties<T> before, SleeperProperties<T> after) {
        return new PropertiesDiff<>(before, after).getChanges();
    }

    private PropertyDiff valueChanged(SleeperProperty property, String before, String after) {
        return new PropertyDiff(property, before, after);
    }

    private PropertyDiff valueDeleted(SleeperProperty property, String value) {
        return new PropertyDiff(property, value, null);
    }

    private PropertyDiff newValue(SleeperProperty property, String value) {
        return new PropertyDiff(property, null, value);
    }

    private PropertyDiff valueChanged(String property, String before, String after) {
        return new PropertyDiff(property, before, after);
    }

    private PropertyDiff valueDeleted(String property, String value) {
        return new PropertyDiff(property, value, null);
    }

    private PropertyDiff newValue(String property, String value) {
        return new PropertyDiff(property, null, value);
    }

    private InstanceProperties createInstanceProperties() {
        return createInstanceProperties("test-instance");
    }

    private InstanceProperties createInstanceProperties(String instanceId) {
        return GenerateInstanceProperties.builder()
                .accountSupplier(() -> "test-account-id").regionProvider(() -> Region.AWS_GLOBAL)
                .instanceId(instanceId).vpcId("some-vpc").subnetId("some-subnet").build().generate();
    }

    private TableProperties createTableProperties() {
        return GenerateTableProperties.from(createInstanceProperties(), schemaWithKey("key"), "test-table");
    }
}
