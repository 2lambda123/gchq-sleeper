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
package sleeper.ingest.impl.partitionfilewriter;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.ingest.impl.ParquetConfiguration;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class DirectPartitionFileWriterFactory implements PartitionFileWriterFactory {

    private final ParquetConfiguration parquetConfiguration;
    private final String filePathPrefix;
    private final Supplier<String> fileNameGenerator;

    private DirectPartitionFileWriterFactory(
            ParquetConfiguration parquetConfiguration, String filePathPrefix,
            Supplier<String> fileNameGenerator) {
        this.parquetConfiguration = Objects.requireNonNull(parquetConfiguration, "parquetWriterConfiguration must not be null");
        this.filePathPrefix = Objects.requireNonNull(filePathPrefix, "filePathPrefix must not be null");
        this.fileNameGenerator = Objects.requireNonNull(fileNameGenerator, "fileNameGenerator must not be null");
    }

    public static DirectPartitionFileWriterFactory from(ParquetConfiguration configuration, String filePathPrefix) {
        return from(configuration, filePathPrefix, () -> UUID.randomUUID().toString());
    }

    public static DirectPartitionFileWriterFactory from(
            ParquetConfiguration configuration, String filePathPrefix,
            Supplier<String> fileNameGenerator) {
        return new DirectPartitionFileWriterFactory(configuration, filePathPrefix, fileNameGenerator);
    }

    public static DirectPartitionFileWriterFactory from(
            ParquetConfiguration configuration,
            InstanceProperties instanceProperties,
            TableProperties tableProperties) {
        return from(configuration,
                instanceProperties.get(FILE_SYSTEM) + instanceProperties.get(DATA_BUCKET) +
                        "/" + tableProperties.get(TABLE_ID));
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new DirectPartitionFileWriter(
                    partition,
                    parquetConfiguration,
                    filePathPrefix,
                    fileNameGenerator.get()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
