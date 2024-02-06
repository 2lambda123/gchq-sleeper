/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.systemtest.drivers.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

public class IngestSourceFilesDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestSourceFilesDriver.class);

    private final IngestSourceContext context;
    private final S3Client s3Client;

    public IngestSourceFilesDriver(IngestSourceContext context, S3Client s3Client) {
        this.context = context;
        this.s3Client = s3Client;
    }

    public String getSourceBucketName() {
        return context.getBucketName();
    }

    public List<String> getIngestJobFilesInBucket(Stream<String> files) {
        return files.map(file -> context.getBucketName() + "/" + file)
                .collect(Collectors.toUnmodifiableList());
    }

    public void writeFile(TableProperties tableProperties, String file, Iterator<Record> records) {
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(
                new org.apache.hadoop.fs.Path("s3a://" + context.getBucketName() + "/" + file), tableProperties, new Configuration())) {
            for (Record record : (Iterable<Record>) () -> records) {
                writer.write(record);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void emptyBucket() {
        List<ObjectIdentifier> objects = s3Client.listObjectsV2Paginator(builder -> builder.bucket(context.getBucketName()))
                .contents().stream().map(S3Object::key)
                .filter(not(InstanceProperties.S3_INSTANCE_PROPERTIES_FILE::equals))
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .collect(Collectors.toList());
        if (!objects.isEmpty()) {
            s3Client.deleteObjects(builder -> builder.bucket(context.getBucketName())
                    .delete(deleteBuilder -> deleteBuilder.objects(objects)));
        }
    }

    public GeneratedIngestSourceFiles findGeneratedFiles() {
        List<S3Object> objects = s3Client.listObjectsV2Paginator(builder ->
                        builder.bucket(context.getBucketName()).prefix("ingest/"))
                .contents().stream().collect(Collectors.toUnmodifiableList());
        LOGGER.info("Found ingest objects in source bucket: {}", objects.size());
        return new GeneratedIngestSourceFiles(context.getBucketName(), objects);
    }

    public static List<String> getS3ObjectJobIds(Stream<String> keys) {
        return keys.map(key -> key.substring("ingest/".length(), key.lastIndexOf('/')))
                .collect(Collectors.toUnmodifiableList());
    }
}
