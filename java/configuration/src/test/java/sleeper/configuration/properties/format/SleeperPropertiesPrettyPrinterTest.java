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
package sleeper.configuration.properties.format;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.InstancePropertyGroup;
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SleeperProperties.loadProperties;

class SleeperPropertiesPrettyPrinterTest {

    private static String printEmptyInstanceProperties() throws IOException {
        return printInstanceProperties("");
    }

    private static String printInstanceProperties(String properties) throws IOException {
        return print(InstanceProperty.getAll(), InstancePropertyGroup.getAll(),
                new InstanceProperties(loadProperties(properties)));
    }

    private static <T extends SleeperProperty> String print(
            List<T> properties, List<PropertyGroup> groups, SleeperProperties<T> values) {
        OutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream, false, StandardCharsets.UTF_8);
        new SleeperPropertiesPrettyPrinter<>(
                properties, groups, printStream::println)
                .print(values);
        return outputStream.toString();
    }

    @Test
    void shouldPrintAllInstanceProperties() throws Exception {
        // When / Then
        assertThat(printEmptyInstanceProperties())
                // Check all the user defined properties are present in the output
                .contains(UserDefinedInstanceProperty.getAll().stream()
                        .map(UserDefinedInstanceProperty::getPropertyName)
                        .collect(Collectors.toList()))
                // Check at least one system-defined property is present in the output
                .containsAnyOf(SystemDefinedInstanceProperty.getAll().stream()
                        .map(SystemDefinedInstanceProperty::getPropertyName)
                        .toArray(String[]::new));
    }

    @Test
    void shouldPrintPropertyValueWithDescription() throws Exception {
        // When / Then
        assertThat(printInstanceProperties("sleeper.account=1234567890"))
                .contains("# The AWS account number. This is the AWS account that the instance will be deployed to.\n" +
                        "sleeper.account: 1234567890\n");
    }

    @Test
    void shouldPrintUnsetPropertyValue() throws Exception {
        // TODO comment out unset property in output
        // TODO ensure we can load properties back from the output and we get the same thing
        // When / Then
        assertThat(printEmptyInstanceProperties())
                .contains("sleeper.logging.root.level: null\n");
    }

    @Test
    void shouldPrintPropertyDescriptionWithMultipleLines() throws Exception {
        // When / Then
        assertThat(printEmptyInstanceProperties())
                .contains("# A file will not be deleted until this number of seconds have passed after it has been marked as\n" +
                        "# ready for garbage collection. The reason for not deleting files immediately after they have been\n" +
                        "# marked as ready for garbage collection is that they may still be in use by queries. This property\n" +
                        "# can be overridden on a per-table basis.\n" +
                        "sleeper.default.gc.delay.seconds");
    }

    @Test
    void shouldPrintPropertyDescriptionWithCustomLineBreaks() throws Exception {
        // When / Then
        assertThat(printEmptyInstanceProperties())
                .contains("# The way in which partition files are written to the main Sleeper store.\n" +
                        "# Valid values are 'direct' (which writes using the s3a Hadoop file system) and 'async' (which writes\n" +
                        "# locally and then copies the completed Parquet file asynchronously into S3).\n" +
                        "# The direct method is simpler but the async method should provide better performance when the number\n" +
                        "# of partitions is large.\n" +
                        "sleeper.ingest.partition.file.writer.type");
    }

    @Test
    void shouldPrintSpacingBetweenProperties() throws Exception {
        // When / Then
        assertThat(printInstanceProperties("" +
                "sleeper.logging.parquet.level=INFO\n" +
                "sleeper.logging.aws.level=INFO\n" +
                "sleeper.logging.root.level=INFO"))
                .contains("# The logging level for Parquet logs.\n" +
                        "sleeper.logging.parquet.level: INFO\n" +
                        "\n" +
                        "# The logging level for AWS logs.\n" +
                        "sleeper.logging.aws.level: INFO\n" +
                        "\n" +
                        "# The logging level for everything else.\n" +
                        "sleeper.logging.root.level: INFO");
    }

    @DisplayName("Format property descriptions")
    @Nested
    class FormatPropertyDescriptions {
        @Test
        void shouldFormatSingleLineString() {
            // Given
            String singleLineString = "Test string that can fit on one line";

            // When
            String formattedString = SleeperPropertiesPrettyPrinter.formatString(singleLineString);

            // Then
            assertThat(formattedString)
                    .isEqualTo("# Test string that can fit on one line");
        }

        @Test
        void shouldFormatAndLineWrapDescription() {
            // Given
            String multiLineString = "Test string that cannot fit on one line, so needs one or more than one lines to fit it all on the screen";

            // When
            String formattedString = SleeperPropertiesPrettyPrinter.formatString(multiLineString);

            // Then
            assertThat(formattedString)
                    .isEqualTo("" +
                            "# Test string that cannot fit on one line, so needs one or more than one lines to fit it all on the\n" +
                            "# screen");
        }

        @Test
        void shouldFormatAndLineWrapDescriptionWithCustomLineBreaks() {
            // Given
            String multiLineString = "Test string that cannot fit on one line\nbut with a custom line break. " +
                    "This is to verify if the line still wraps even after after a custom line break";

            // When
            String formattedString = SleeperPropertiesPrettyPrinter.formatString(multiLineString);

            // Then
            assertThat(formattedString)
                    .isEqualTo("" +
                            "# Test string that cannot fit on one line\n" +
                            "# but with a custom line break. This is to verify if the line still wraps even after after a custom\n" +
                            "# line break");
        }
    }
}
