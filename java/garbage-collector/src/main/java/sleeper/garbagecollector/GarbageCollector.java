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
package sleeper.garbagecollector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIdentity;
import sleeper.core.util.LoggedDuration;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;

/**
 * Queries the {@link StateStore} for files that are marked as being ready for
 * garbage collection, and deletes them.
 */
public class GarbageCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(GarbageCollector.class);

    private final Configuration conf;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final int garbageCollectorBatchSize;

    public GarbageCollector(Configuration conf,
                            TablePropertiesProvider tablePropertiesProvider,
                            StateStoreProvider stateStoreProvider,
                            int garbageCollectorBatchSize) {
        this.conf = conf;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.garbageCollectorBatchSize = garbageCollectorBatchSize;
    }

    public void run() throws StateStoreException, IOException {
        runAtTime(Instant.now());
    }

    public void runAtTime(Instant startTime) throws StateStoreException, IOException {
        int totalDeleted = 0;
        List<TableProperties> tables = tablePropertiesProvider.streamAllTables()
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Obtained list of {} tables", tables.size());

        for (TableProperties tableProperties : tables) {
            TableIdentity tableId = tableProperties.getId();
            LOGGER.info("Obtaining StateStore for table {}", tableId);
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);

            LOGGER.debug("Requesting iterator of files ready for garbage collection from state store");
            int delayBeforeDeletion = tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION);
            Instant deletionTime = startTime.minus(delayBeforeDeletion, ChronoUnit.MINUTES);
            Iterator<String> readyForGC = stateStore.getReadyForGCFilenamesBefore(deletionTime).iterator();

            int numberDeleted = 0;
            while (readyForGC.hasNext() && numberDeleted < garbageCollectorBatchSize) {
                String filename = readyForGC.next();
                deleteFileAndUpdateStateStore(filename, stateStore, conf);
                numberDeleted++;
            }
            LOGGER.info("{} files deleted for table {}", numberDeleted, tableId);
            totalDeleted += numberDeleted;
        }
        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
        LOGGER.info("{} files deleted in {}", totalDeleted, duration);
    }

    private void deleteFileAndUpdateStateStore(String filename, StateStore stateStore, Configuration conf) throws IOException {
        deleteFiles(filename, conf);
        try {
            stateStore.deleteReadyForGCFile(filename);
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating status of " + filename + " to garbage collected", e);
        }
    }

    private void deleteFiles(String filename, Configuration conf) throws IOException {
        deleteFile(filename, conf);
        String sketchesFile = filename.replace(".parquet", ".sketches");
        deleteFile(sketchesFile, conf);
    }

    private void deleteFile(String filename, Configuration conf) throws IOException {
        Path path = new Path(filename);
        FileSystem fileSystem = path.getFileSystem(conf);
        try {
            if (!fileSystem.exists(path)) {
                LOGGER.warn("File did not exist: {}", filename);
                return;
            }
            boolean success = path.getFileSystem(conf).delete(path, false);
            if (!success) {
                LOGGER.warn("File could not be deleted: {}", filename);
                return;
            }
            LOGGER.info("Deleted file {}", filename);
        } catch (IOException e) {
            LOGGER.info("Failed to delete file {}", filename, e);
        }
    }
}
