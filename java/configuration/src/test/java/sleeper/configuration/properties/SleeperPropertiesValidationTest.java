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

package sleeper.configuration.properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class SleeperPropertiesValidationTest {
    @DisplayName("Trigger validation")
    @Nested
    class TriggerValidation {

        private InstanceProperties invalidInstanceProperties() {
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "-1");
            return instanceProperties;
        }

        private TableProperties invalidTableProperties(InstanceProperties instanceProperties) {
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.set(COMPRESSION_CODEC, "madeUp");
            return tableProperties;
        }

        @Test
        void shouldThrowExceptionOnLoadIfInstancePropertiesValidationFails() throws Exception {
            // Given
            String serialised = invalidInstanceProperties().saveAsString();

            // When / Then
            InstanceProperties properties = new InstanceProperties();
            assertThatThrownBy(() -> properties.loadFromString(serialised))
                    .isInstanceOf(SleeperPropertyInvalidException.class);
        }

        @Test
        void shouldThrowExceptionOnLoadIfTablePropertiesValidationFails() throws IOException {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            String serialised = invalidTableProperties(instanceProperties).saveAsString();

            // When / Then
            TableProperties properties = new TableProperties(instanceProperties);
            assertThatThrownBy(() -> properties.loadFromString(serialised))
                    .isInstanceOf(SleeperPropertyInvalidException.class);
        }

        @Test
        void shouldNotValidateWhenConstructingInstanceProperties() throws IOException {
            // Given
            Properties properties = loadProperties(invalidInstanceProperties().saveAsString());

            // When / Then
            assertThatCode(() -> new InstanceProperties(properties))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldNotValidateWhenConstructingTableProperties() throws IOException {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            Properties properties = loadProperties(invalidTableProperties(instanceProperties).saveAsString());

            // When / Then
            assertThatCode(() -> new TableProperties(instanceProperties, properties))
                    .doesNotThrowAnyException();
        }
    }

    @DisplayName("Validate instance properties")
    @Nested
    class ValidateInstanceProperties {
        @Test
        void shouldFailValidationIfRequiredPropertyIsMissing() {
            // Given - no account set
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.set(REGION, "eu-west-2");
            instanceProperties.set(JARS_BUCKET, "jars");
            instanceProperties.set(VERSION, "0.1");
            instanceProperties.set(ID, "test");
            instanceProperties.set(VPC_ID, "aVPC");
            instanceProperties.set(SUBNET, "subnet1");

            // When / Then
            assertThatThrownBy(instanceProperties::validate)
                    .hasMessageContaining(ACCOUNT.getPropertyName());
        }

        @Test
        void shouldFailValidationIfPropertyIsInvalid() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "-1");

            // When / Then
            assertThatThrownBy(instanceProperties::validate)
                    .hasMessageContaining(MAXIMUM_CONNECTIONS_TO_S3.getPropertyName());
        }
    }

    @DisplayName("Validate table properties")
    @Nested
    class ValidateTableProperties {
        @Test
        void shouldFailValidationIfCompressionCodecIsInvalid() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.set(COMPRESSION_CODEC, "madeUp");
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .hasMessage("Property sleeper.table.compression.codec was invalid. It was \"madeUp\"");
        }

        @Test
        void shouldFailValidationIfTableNameIsAbsent() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.unset(TABLE_NAME);
            // When / Then
            assertThatThrownBy(tableProperties::validate)
                    .hasMessage("Property sleeper.table.name was invalid. It was \"null\"");
        }

        @Test
        void shouldFailValidationIfCompactionFilesBatchSizeTooLargeForDynamoDBStateStore() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.dynamodb.DynamoDBStateStore");
            tableProperties.setNumber(COMPACTION_FILES_BATCH_SIZE, 49);

            // When/Then
            assertThatThrownBy(tableProperties::validate)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Property sleeper.table.compaction.files.batch.size was invalid. " +
                            "It was \"49\"");
        }

        @Test
        void shouldPassValidationIfCompactionFilesBatchSizeTooLargeForDynamoDBStateStoreButS3StateStoreChosen() {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
            tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.s3.S3StateStore");
            tableProperties.setNumber(COMPACTION_FILES_BATCH_SIZE, 49);

            // When/Then
            assertThatCode(tableProperties::validate).doesNotThrowAnyException();
        }
    }
}
