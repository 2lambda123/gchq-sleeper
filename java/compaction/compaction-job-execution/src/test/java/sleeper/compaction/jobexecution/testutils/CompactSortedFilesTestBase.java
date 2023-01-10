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
package sleeper.compaction.jobexecution.testutils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.compaction.job.CompactionJobFactory;
import sleeper.core.CommonTestConstants;

import java.io.File;

import static java.nio.file.Files.createTempDirectory;

public class CompactSortedFilesTestBase {
    public static final String DEFAULT_TASK_ID = "task-id";
    @TempDir
    public File folder = CommonTestConstants.TMP_DIRECTORY;
    protected String folderName;

    @BeforeEach
    public void setUpBase() throws Exception {
        folderName = createTempDirectory(folder.toPath(), null).toString();
    }

    protected CompactionJobFactory compactionFactory() {
        return compactionFactoryBuilder().build();
    }

    protected CompactionJobFactory.Builder compactionFactoryBuilder() {
        return CompactionJobFactory.withTableName("table").outputFilePrefix(folderName);
    }
}
