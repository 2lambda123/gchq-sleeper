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
package sleeper.statestore;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import sleeper.core.statestore.AllReferencesToAFile;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Reads and writes the state of files in a state store to an Arrow file.
 */
public class StateStoreFilesArrowFormat {

    private static final Field FILENAME = Field.notNullable("filename", Utf8.INSTANCE);
    private static final Field UPDATE_TIME = Field.notNullable("updateTime", Types.MinorType.TIMESTAMPMILLI.getType());
    private static final Field PARTITION_ID = Field.notNullable("partitionId", Utf8.INSTANCE);
    private static final Field REFERENCE_UPDATE_TIME = Field.notNullable("updateTime", Types.MinorType.TIMESTAMPMILLI.getType());
    private static final Field JOB_ID = Field.nullable("jobId", Utf8.INSTANCE);
    private static final Field NUMBER_OF_RECORDS = Field.notNullable("numberOfRecords", Types.MinorType.UINT8.getType());
    private static final Field COUNT_APPROXIMATE = Field.notNullable("countApproximate", Types.MinorType.BIT.getType());
    private static final Field ONLY_CONTAINS_DATA_FOR_THIS_PARTITION = Field.notNullable("onlyContainsDataForThisPartition", Types.MinorType.BIT.getType());
    private static final Field REFERENCE = new Field("reference",
            FieldType.notNullable(Types.MinorType.STRUCT.getType()),
            List.of(PARTITION_ID, REFERENCE_UPDATE_TIME, JOB_ID,
                    NUMBER_OF_RECORDS, COUNT_APPROXIMATE, ONLY_CONTAINS_DATA_FOR_THIS_PARTITION));
    private static final Field REFERENCES = new Field("partitionReferences",
            FieldType.notNullable(Types.MinorType.LIST.getType()), List.of(REFERENCE));
    private static final Schema FILES_SCHEMA = new Schema(List.of(FILENAME, UPDATE_TIME, REFERENCES));

    private StateStoreFilesArrowFormat() {
    }

    /**
     * Writes the state of files in Arrow format.
     *
     * @param  files       the files in the state store
     * @param  allocator   the buffer allocator
     * @param  channel     the channel
     * @throws IOException if writing to the channel fails
     */
    public static void write(Collection<AllReferencesToAFile> files, BufferAllocator allocator, WritableByteChannel channel) throws IOException {
        try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(FILES_SCHEMA, allocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, channel)) {
            vectorSchemaRoot.getFieldVectors().forEach(fieldVector -> fieldVector.setInitialCapacity(files.size()));
            vectorSchemaRoot.allocateNew();
            writer.start();
            int rowNumber = 0;
            for (AllReferencesToAFile file : files) {
                VarCharVector filenameVector = (VarCharVector) vectorSchemaRoot.getVector(FILENAME);
                filenameVector.setSafe(rowNumber, file.getFilename().getBytes(StandardCharsets.UTF_8));
                TimeStampMilliVector updateTimeVector = (TimeStampMilliVector) vectorSchemaRoot.getVector(UPDATE_TIME);
                updateTimeVector.setSafe(rowNumber, file.getLastStateStoreUpdateTime().toEpochMilli());
                ListVector referencesVector = (ListVector) vectorSchemaRoot.getVector(REFERENCES);
                writeReferences(file, rowNumber, referencesVector.getWriter());
                rowNumber++;
                vectorSchemaRoot.setRowCount(rowNumber);
            }
            writer.writeBatch();
            writer.end();
        }
    }

    private static void writeReferences(AllReferencesToAFile file, int fileNumber, UnionListWriter writer) {
        writer.startList();
        writer.setPosition(fileNumber);
        writer.endList();
    }

    /**
     * Reads the state of files from Arrow format.
     *
     * @param  channel the channel
     * @return         the files in the state store
     */
    public static List<AllReferencesToAFile> read(BufferAllocator allocator, ReadableByteChannel channel) throws IOException {
        List<AllReferencesToAFile> files = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(channel, allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
            VarCharVector filenameVector = (VarCharVector) vectorSchemaRoot.getVector(FILENAME);
            TimeStampMilliVector updateTimeVector = (TimeStampMilliVector) vectorSchemaRoot.getVector(UPDATE_TIME);
            for (int index = 0; index < vectorSchemaRoot.getRowCount(); index++) {
                String filename = filenameVector.getObject(index).toString();
                Instant updateTime = Instant.ofEpochMilli(updateTimeVector.get(index));
                files.add(AllReferencesToAFile.builder()
                        .filename(filename)
                        .lastStateStoreUpdateTime(updateTime)
                        .internalReferences(List.of())
                        .build());
            }
        }
        return files;
    }
}
