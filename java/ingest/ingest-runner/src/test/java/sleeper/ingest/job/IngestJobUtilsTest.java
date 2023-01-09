/*
 * Copyright 2023 Crown Copyright
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
package sleeper.ingest.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import sleeper.core.CommonTestConstants;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestJobUtilsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void shouldReturnEmptyListIfNoFiles() throws Exception {
        // Given
        Configuration conf = new Configuration();

        // When
        List<Path> pathsForIngest = IngestJobUtils.getPaths(new ArrayList<>(), conf, "");

        // Then
        assertThat(pathsForIngest).isEmpty();
    }

    @Test
    public void shouldReturnEmptyListIfNull() throws Exception {
        // Given
        Configuration conf = new Configuration();

        // When
        List<Path> pathsForIngest = IngestJobUtils.getPaths(null, conf, "");

        // Then
        assertThat(pathsForIngest).isEmpty();
    }

    @Test
    public void shouldGetPathsForMultipleIndividualParquetFilesInOneDir() throws Exception {
        // Given
        String localDir = folder.newFolder().getAbsolutePath();
        Configuration conf = new Configuration();
        List<String> files = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            String outputFile = localDir + "/file-" + i + ".parquet";
            Files.createFile(Paths.get(outputFile));
            files.add(outputFile);
        }

        // When
        List<Path> pathsForIngest = IngestJobUtils.getPaths(files, conf, "");

        // Then
        assertThat(pathsForIngest)
                .extracting(path -> path.toUri().getPath())
                .containsExactlyInAnyOrder(localDir + "/file-0.parquet", localDir + "/file-1.parquet");
    }

    @Test
    public void shouldGetPathsForIndividualFilesThatAreNotCrcFilesInOneDir() throws Exception {
        // Given
        String localDir = folder.newFolder().getAbsolutePath();
        Configuration conf = new Configuration();
        List<String> files = new ArrayList<>();

        Files.createFile(Paths.get(localDir + "/file-0.parquet"));
        Files.createFile(Paths.get(localDir + "/file-1.crc"));
        Files.createFile(Paths.get(localDir + "/file-2.csv"));

        files.add(localDir + "/file-0.parquet");
        files.add(localDir + "/file-1.crc");
        files.add(localDir + "/file-2.csv");

        // When
        List<Path> pathsForIngest = IngestJobUtils.getPaths(files, conf, "");

        // Then
        assertThat(pathsForIngest)
                .extracting(path -> path.toUri().getPath())
                .containsExactlyInAnyOrder(localDir + "/file-0.parquet", localDir + "/file-2.csv");
    }

    @Test
    public void shouldGetPathsForFilesInMultipleDirectories() throws Exception {
        // Given
        String localDir = folder.newFolder().getAbsolutePath();
        Configuration conf = new Configuration();
        List<String> files = new ArrayList<>();

        for (int dir = 0; dir < 3; dir++) {
            Files.createDirectory(Paths.get(localDir + "/dir-" + dir));
            for (int i = 0; i < 2; i++) {
                String outputFile = localDir + "/dir-" + dir + "/file-" + i + ".parquet";
                Files.createFile(Paths.get(outputFile));
            }
            //provide all the sub-directories
            files.add(localDir + "/dir-" + dir + "/");
        }

        // When
        List<Path> pathsForIngest = IngestJobUtils.getPaths(files, conf, "");

        // Then
        assertThat(pathsForIngest)
                .extracting(path -> path.toUri().getPath())
                .containsExactlyInAnyOrder(
                        localDir + "/dir-0/file-0.parquet", localDir + "/dir-0/file-1.parquet",
                        localDir + "/dir-1/file-0.parquet", localDir + "/dir-1/file-1.parquet",
                        localDir + "/dir-2/file-0.parquet", localDir + "/dir-2/file-1.parquet");
    }

    @Test
    public void shouldGetPathsForFilesInNestedDirectories() throws Exception {
        // Given
        String localDir = folder.newFolder().getAbsolutePath();
        Configuration conf = new Configuration();
        List<String> files = new ArrayList<>();

        for (int dir = 0; dir < 2; dir++) {
            Files.createDirectory(Paths.get(localDir + "/dir-" + dir));
            for (int i = 0; i < 2; i++) {
                String outputFile = localDir + "/dir-" + dir + "/file-" + i + ".parquet";
                Files.createFile(Paths.get(outputFile));
            }
        }

        Files.createDirectory(Paths.get(localDir + "/dir-0" + "/dir-nested"));
        String outputFile = localDir + "/dir-0/dir-nested/file-0.parquet";
        Files.createFile(Paths.get(outputFile));

        //provide only the highest directory
        files.add(localDir);

        // When
        List<Path> pathsForIngest = IngestJobUtils.getPaths(files, conf, "");

        // Then
        assertThat(pathsForIngest)
                .extracting(path -> path.toUri().getPath())
                .containsExactlyInAnyOrder(
                        localDir + "/dir-0/file-0.parquet", localDir + "/dir-0/file-1.parquet",
                        localDir + "/dir-0/dir-nested/file-0.parquet",
                        localDir + "/dir-1/file-0.parquet", localDir + "/dir-1/file-1.parquet");
    }
}
