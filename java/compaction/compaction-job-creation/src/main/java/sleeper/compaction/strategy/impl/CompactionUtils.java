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
package sleeper.compaction.strategy.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CompactionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionUtils.class);

    private CompactionUtils() {
    }

    public static List<FileInfo> getFilesInAscendingOrder(Partition partition, List<FileInfo> fileInfos) {
        // Get files in this partition
        List<FileInfo> files = fileInfos
                .stream()
                .filter(f -> f.getPartitionId().equals(partition.getId()))
                .collect(Collectors.toList());
        LOGGER.info("Creating jobs for leaf partition {}", partition);
        LOGGER.info("There are {} files for this partition", files.size());

        // Create map of number of records in file to files, sorted by number of records in file
        SortedMap<Long, List<FileInfo>> linesToFiles = new TreeMap<>();
        for (FileInfo fileInfo : files) {
            if (!linesToFiles.containsKey(fileInfo.getNumberOfRecords())) {
                linesToFiles.put(fileInfo.getNumberOfRecords(), new ArrayList<>());
            }
            linesToFiles.get(fileInfo.getNumberOfRecords()).add(fileInfo);
        }

        // Convert to list of FileInfos in ascending order of number of lines
        List<FileInfo> fileInfosList = new ArrayList<>();
        for (Map.Entry<Long, List<FileInfo>> entry : linesToFiles.entrySet()) {
            fileInfosList.addAll(entry.getValue());
        }

        return fileInfosList;
    }
}
