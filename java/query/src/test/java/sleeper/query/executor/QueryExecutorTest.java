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
package sleeper.query.executor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.QueryException;
import sleeper.query.model.Query;
import sleeper.query.recordretrieval.InMemoryLeafPartitionRecordRetriever;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.IMMUTABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;


public class QueryExecutorTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final InMemoryLeafPartitionRecordRetriever recordStore = new InMemoryLeafPartitionRecordRetriever();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);

    @Nested
    @DisplayName("Query records")
    class QueryRecords {

        @Test
        void shouldReturnSubRangeInSinglePartition() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When
            List<Record> records = getRecords(queryRange(100L, 200L));

            // Then
            assertThat(records).containsExactly(new Record(Map.of("key", 123L)));
        }

        @Test
        void shouldReturnRecordInOneOfTwoRanges() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When
            List<Record> records = getRecords(queryRegions(range(100L, 200L), range(400L, 500L)));

            // Then
            assertThat(records).containsExactly(new Record(Map.of("key", 123L)));
        }

        @Test
        void shouldNotFindRecordOutsideSubRangeInSinglePartition() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When
            List<Record> records = getRecords(queryRange(200L, 300L));

            // Then
            assertThat(records).isEmpty();
        }

        @Test
        void shouldNotFindRecordOutsidePartitionRangeWhenFileContainsAnInactiveRecord() throws Exception {
            // Given
            stateStore.initialise(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 5L)
                    .buildList());
            addPartitionFile("R", "file.parquet", List.of(
                    new Record(Map.of("key", 2L)),
                    new Record(Map.of("key", 7L))));

            // When
            List<Record> records = getRecords(queryAllRecords());

            // Then
            assertThat(records).containsExactly(
                    new Record(Map.of("key", 7L)));
        }
    }

    @Nested
    @DisplayName("Reinitialise based on a timeout")
    class ReinitialiseOnTimeout {

        @Test
        public void shouldReloadActiveFilesFromStateStoreWhenTimedOut() throws Exception {
            // Given files are added after the executor is first initialised
            tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
            QueryExecutor queryExecutor = executorAtTime(Instant.parse("2023-11-27T09:30:00Z"));
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When the first initialisation has expired
            queryExecutor.initIfNeeded(Instant.parse("2023-11-27T09:35:00Z"));

            // Then the records that were added are found
            assertThat(getRecords(queryExecutor, queryAllRecords()))
                    .containsExactly(new Record(Map.of("key", 123L)));
        }

        @Test
        public void shouldNotReloadActiveFilesBeforeTimeOut() throws Exception {
            // Given files are added after the executor is first initialised
            tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
            QueryExecutor queryExecutor = executorAtTime(Instant.parse("2023-11-27T09:30:00Z"));
            addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

            // When the first initialisation has not yet expired
            queryExecutor.initIfNeeded(Instant.parse("2023-11-27T09:31:00Z"));

            // Then the records that were added are not found
            assertThat(getRecords(queryExecutor, queryAllRecords())).
                    isEmpty();
        }
    }

    private void addRootFile(String filename, List<Record> records) {
        addFile(fileInfoFactory().rootFile(filename, records.size()), records);
    }

    private void addPartitionFile(String partitionId, String filename, List<Record> records) {
        addFile(fileInfoFactory().partitionFile(partitionId, filename, records.size()), records);
    }

    private void addFile(FileInfo fileInfo, List<Record> records) {
        try {
            stateStore.addFile(fileInfo);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        recordStore.addFile(fileInfo.getFilename(), records);
    }

    private QueryExecutor executor() throws Exception {
        return executorAtTime(Instant.now());
    }

    private QueryExecutor executorAtTime(Instant time) throws Exception {
        QueryExecutor executor = uninitialisedExecutorAtTime(time);
        executor.init(time);
        return executor;
    }

    private QueryExecutor uninitialisedExecutorAtTime(Instant time) {
        return new QueryExecutor(ObjectFactory.noUserJars(), stateStore, tableProperties, recordStore, time);
    }

    private List<Record> getRecords(Query query) throws Exception {
        return getRecords(executor(), query);
    }

    private List<Record> getRecords(QueryExecutor executor, Query query) {
        try (var it = executor.execute(query)) {
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, IMMUTABLE), false)
                    .collect(Collectors.toUnmodifiableList());
        } catch (QueryException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Query.Builder query() {
        return Query.builder().queryId(UUID.randomUUID().toString())
                .tableName(tableProperties.get(TABLE_NAME));
    }

    private Query queryAllRecords() {
        return query()
                .regions(List.of(partitionTree().getRootPartition().getRegion()))
                .build();
    }

    private Query queryRange(Object min, Object max) {
        return queryRegions(range(min, max));
    }

    private Query queryRegions(Region... regions) {
        return query()
                .regions(List.of(regions))
                .build();
    }

    private Region range(Object min, Object max) {
        Field field = tableProperties.getSchema().getField("key").orElseThrow();
        return new Region(new Range(field, min, max));
    }

    private PartitionTree partitionTree() {
        try {
            return new PartitionTree(tableProperties.getSchema(), stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private FileInfoFactory fileInfoFactory() {
        return FileInfoFactory.from(tableProperties.getSchema(), stateStore);
    }
}
