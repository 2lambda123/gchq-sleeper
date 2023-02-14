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
package sleeper.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.WordUtils;

import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class ClientUtils {

    private ClientUtils() {
    }

    public static InstanceProperties getInstanceProperties(String instanceId) throws IOException {
        return getInstanceProperties(AmazonS3ClientBuilder.defaultClient(), instanceId);
    }

    public static InstanceProperties getInstanceProperties(AmazonS3 amazonS3, String instanceId) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(amazonS3, instanceId);
        return instanceProperties;
    }

    public static Optional<String> optionalArgument(String[] args, int index) {
        if (args.length > index) {
            return Optional.of(args[index]);
        } else {
            return Optional.empty();
        }
    }

    private static final long K_COUNT = 1_000;
    private static final long M_COUNT = 1_000_000;
    private static final long G_COUNT = 1_000_000_000;
    private static final long T_COUNT = 1_000_000_000_000L;

    public static String abbreviatedRecordCount(long records) {
        if (records < K_COUNT) {
            return "" + records;
        } else if (records < M_COUNT) {
            return Math.round((double) records / K_COUNT) + "K (" + countWithCommas(records) + ")";
        } else if (records < G_COUNT) {
            return Math.round((double) records / M_COUNT) + "M (" + countWithCommas(records) + ")";
        } else if (records < T_COUNT) {
            return Math.round((double) records / G_COUNT) + "G (" + countWithCommas(records) + ")";
        } else {
            return countWithCommas(Math.round((double) records / T_COUNT)) + "T (" + countWithCommas(records) + ")";
        }
    }

    public static String countWithCommas(long count) {
        return splitNonDecimalIntoParts("" + count);
    }

    public static String decimalWithCommas(String formatStr, double decimal) {
        String str = String.format(formatStr, decimal);
        int decimalIndex = str.indexOf('.');
        if (decimalIndex > 0) {
            return splitNonDecimalIntoParts(str.substring(0, decimalIndex)) + str.substring(decimalIndex);
        } else {
            return splitNonDecimalIntoParts(str);
        }
    }

    private static String splitNonDecimalIntoParts(String str) {
        int length = str.length();
        int firstPartEnd = length % 3;

        List<String> parts = new ArrayList<>();
        if (firstPartEnd != 0) {
            parts.add(str.substring(0, firstPartEnd));
        }
        for (int i = firstPartEnd; i < length; i += 3) {
            parts.add(str.substring(i, i + 3));
        }
        return String.join(",", parts);
    }

    public static String formatPropertyDescription(String str) {
        return Arrays.stream(str.split("\n")).
                map(line -> "# " + WordUtils.wrap(line, 100).replace("\n", "\n# "))
                .collect(Collectors.joining("\n"));
    }

    public static void clearDirectory(Path tempDir) {
        try (Stream<Path> paths = Files.walk(tempDir)) {
            paths.skip(1).sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Path.getFileName returns null when path has no elements
    // but throwing a null pointer exception in this method is fine
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public static List<Path> listJarsInDirectory(Path directory) throws IOException {
        try (Stream<Path> jars = Files.list(directory)) {
            return jars.filter(path -> path.getFileName().toString().endsWith(".jar")).collect(Collectors.toList());
        }
    }

    public static int runCommand(String... commands) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(commands).inheritIO().start();

        return process.waitFor();
    }
}
