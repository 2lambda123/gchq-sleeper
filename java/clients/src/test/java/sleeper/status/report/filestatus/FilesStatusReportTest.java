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
package sleeper.status.report.filestatus;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfo;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class FilesStatusReportTest {

    @Test
    public void shouldReportFilesStatusGivenOneActiveFilePerLeafPartition() throws Exception {
        // Given
        Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        List<Partition> partitions = new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H"),
                        Arrays.asList("aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"))
                .parentJoining("I", "A", "B")
                .parentJoining("J", "I", "C")
                .parentJoining("K", "J", "D")
                .parentJoining("L", "K", "E")
                .parentJoining("M", "L", "F")
                .parentJoining("N", "M", "G")
                .parentJoining("O", "N", "H")
                .buildList();
        FileInfoFactory fileInfoFactory = new FileInfoFactory(schema, partitions, lastStateStoreUpdate);
        List<FileInfo> activeFiles = Arrays.asList(
                fileInfoFactory.leafFile(50000001, "123", "456"),
                fileInfoFactory.leafFile(50000002, "abc", "az"),
                fileInfoFactory.leafFile(50000003, "bcd", "bz"),
                fileInfoFactory.leafFile(50000004, "cde", "cz"),
                fileInfoFactory.leafFile(50000005, "def", "dz"),
                fileInfoFactory.leafFile(50000006, "efg", "ez"),
                fileInfoFactory.leafFile(50000007, "fgh", "fz"),
                fileInfoFactory.leafFile(50000008, "ghi", "gz"));

        // When
        FileStatus status = FileStatusCollector.run(StateStoreFiles.builder()
                .partitions(partitions).active(activeFiles)
                .readyForGC(StateStoreReadyForGC.none())
                .build());

        // Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/standard/oneActiveFilePerLeaf.txt"));
        assertThatJson(status.verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/json/oneActiveFilePerLeaf.json"));
    }

    @Test
    public void shouldReportFilesStatusGivenActiveFileInLeafAndMiddlePartition() throws Exception {
        // Given
        Instant lastStateStoreUpdate = Instant.parse("2022-08-22T14:20:00.001Z");
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        List<Partition> partitions = new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B", "C"),
                        Arrays.asList("beeblebrox", "wowbagger"))
                .parentJoining("D", "A", "B")
                .parentJoining("E", "D", "C")
                .buildList();
        FileInfoFactory fileInfoFactory = new FileInfoFactory(schema, partitions, lastStateStoreUpdate);
        List<FileInfo> activeFiles = Arrays.asList(
                fileInfoFactory.leafFile(50000001, "aardvark", "arthur"),
                fileInfoFactory.middleFile(50000002, "arthur dent", "beeblebrox, zaphod"));

        // When
        FileStatus status = FileStatusCollector.run(StateStoreFiles.builder()
                .partitions(partitions).active(activeFiles)
                .readyForGC(StateStoreReadyForGC.none())
                .build());

        // Then
        assertThat(status.verboseReportString(StandardFileStatusReporter::new))
                .isEqualTo(example("reports/standard/leafAndMiddleFile.txt"));
        assertThatJson(status.verboseReportString(JsonFileStatusReporter::new))
                .isEqualTo(example("reports/json/leafAndMiddleFile.json"));
    }

    private static String example(String path) throws IOException {
        URL url = FilesStatusReportTest.class.getClassLoader().getResource(path);
        return IOUtils.toString(Objects.requireNonNull(url), Charset.defaultCharset());
    }

}
