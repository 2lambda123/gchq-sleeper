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
package sleeper.ingest.impl;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.localstack.LocalStackContainer;

import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.IteratorException;
import sleeper.core.key.Key;
import sleeper.core.record.Record;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.PartitionedTableCreator;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;

public class IngestCoordinatorBespokeUsingDirectWriteBackedByArrayListIT {
    @ClassRule
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.DYNAMODB);
    private static final String DATA_BUCKET_NAME = "databucket";
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeEach
    public void before() {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(DATA_BUCKET_NAME);
    }

    @AfterEach
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(0L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerifyUsingDirectWriteBackedByArrayList(
                recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                5,
                1000L);
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(0L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 20),
                        new AbstractMap.SimpleEntry<>(1, 20))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerifyUsingDirectWriteBackedByArrayList(
                recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                5,
                10L);
    }

    private void ingestAndVerifyUsingDirectWriteBackedByArrayList(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            int maxNoOfRecordsInMemory,
            long maxNoOfRecordsInLocalStore) throws IOException, StateStoreException, IteratorException {
        StateStore stateStore = PartitionedTableCreator.createStateStore(
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                recordListAndSchema.sleeperSchema,
                keyAndDimensionToSplitOnInOrder);
        String ingestLocalWorkingDirectory = temporaryFolder.newFolder().getAbsolutePath();

        ParquetConfiguration parquetConfiguration = parquetConfiguration(
                recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
        try (IngestCoordinator<Record> ingestCoordinator = standardIngestCoordinator(
                stateStore, recordListAndSchema.sleeperSchema,
                ArrayListRecordBatchFactory.builder()
                        .parquetConfiguration(parquetConfiguration)
                        .localWorkingDirectory(ingestLocalWorkingDirectory)
                        .maxNoOfRecordsInMemory(maxNoOfRecordsInMemory)
                        .maxNoOfRecordsInLocalStore(maxNoOfRecordsInLocalStore)
                        .buildAcceptingRecords(),
                DirectPartitionFileWriterFactory.from(
                        parquetConfiguration, "s3a://" + DATA_BUCKET_NAME))) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }

        ResultVerifier.verify(
                stateStore,
                recordListAndSchema.sleeperSchema,
                keyToPartitionNoMappingFn,
                recordListAndSchema.recordList,
                partitionNoToExpectedNoOfFilesMap,
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                ingestLocalWorkingDirectory);
    }
}
