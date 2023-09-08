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

package sleeper.systemtest.drivers.util;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ReadRecordsFromS3 {

    private ReadRecordsFromS3() {
    }

    public static Stream<Record> getRecords(Schema schema, S3ObjectSummary s3ObjectSummary) {
        String path = "s3a://" + s3ObjectSummary.getBucketName() + "/" + s3ObjectSummary.getKey();
        List<Record> records = new ArrayList<>();
        try (ParquetRecordReader reader = new ParquetRecordReader(new org.apache.hadoop.fs.Path(path), schema)) {
            new ParquetReaderIterator(reader).forEachRemaining(records::add);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return records.stream();
    }
}
