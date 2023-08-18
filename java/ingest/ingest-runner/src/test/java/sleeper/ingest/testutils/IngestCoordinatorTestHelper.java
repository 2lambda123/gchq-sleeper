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

package sleeper.ingest.testutils;

import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;

import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;

public class IngestCoordinatorTestHelper {
    private IngestCoordinatorTestHelper() {
    }

    public static ParquetConfiguration parquetConfiguration(IngestCoordinatorTestParameters parameters) {
        return parquetConfiguration(parameters.getSchema(), parameters.getHadoopConfiguration());
    }

    public static ParquetConfiguration parquetConfiguration(Schema schema, Configuration hadoopConfiguration) {
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(COMPRESSION_CODEC, "zstd");
        tableProperties.setSchema(schema);
        return ParquetConfiguration.builder()
                .tableProperties(tableProperties)
                .hadoopConfiguration(hadoopConfiguration)
                .build();
    }

    public static <T> IngestCoordinator<T> standardIngestCoordinator(
            StateStore stateStore, Schema schema,
            RecordBatchFactory<T> recordBatchFactory, PartitionFileWriterFactory partitionFileWriterFactory) {
        return standardIngestCoordinatorBuilder(stateStore, schema, recordBatchFactory, partitionFileWriterFactory).build();
    }

    public static <T> IngestCoordinator.Builder<T> standardIngestCoordinatorBuilder(
            IngestCoordinatorTestParameters parameters,
            RecordBatchFactory<T> recordBatchFactory, PartitionFileWriterFactory partitionFileWriterFactory) {
        return standardIngestCoordinatorBuilder(parameters.getStateStore(), parameters.getSchema(),
                recordBatchFactory, partitionFileWriterFactory)
                .iteratorClassName(parameters.getIteratorClassName());
    }

    public static <T> IngestCoordinator.Builder<T> standardIngestCoordinatorBuilder(
            StateStore stateStore, Schema schema,
            RecordBatchFactory<T> recordBatchFactory, PartitionFileWriterFactory partitionFileWriterFactory) {
        return IngestCoordinator.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .ingestPartitionRefreshFrequencyInSeconds(Integer.MAX_VALUE)
                .stateStore(stateStore)
                .schema(schema)
                .recordBatchFactory(recordBatchFactory)
                .partitionFileWriterFactory(partitionFileWriterFactory);
    }
}
