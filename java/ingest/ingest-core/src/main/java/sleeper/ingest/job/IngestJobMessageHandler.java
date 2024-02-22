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

package sleeper.ingest.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobValidatedEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobRejected;

public class IngestJobMessageHandler<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobMessageHandler.class);
    private final TableIndex tableIndex;
    private final IngestJobStatusStore ingestJobStatusStore;
    private final Function<String, T> deserialiser;
    private final Function<T, IngestJob> toIngestJob;
    private final BiFunction<T, IngestJob, T> applyIngestJobChanges;
    private final Function<List<String>, List<String>> expandDirectories;
    private final Supplier<String> jobIdSupplier;
    private final Supplier<Instant> timeSupplier;

    private IngestJobMessageHandler(Builder<T> builder) {
        tableIndex = Objects.requireNonNull(builder.tableIndex, "tableIndex must not be null");
        ingestJobStatusStore = Objects.requireNonNull(builder.ingestJobStatusStore, "ingestJobStatusStore must not be null");
        deserialiser = Objects.requireNonNull(builder.deserialiser, "deserialiser must not be null");
        toIngestJob = Objects.requireNonNull(builder.toIngestJob, "toIngestJob must not be null");
        applyIngestJobChanges = Objects.requireNonNull(builder.applyIngestJobChanges, "applyIngestJobChanges must not be null");
        expandDirectories = Objects.requireNonNull(builder.expandDirectories, "expandDirectories must not be null");
        jobIdSupplier = Objects.requireNonNull(builder.jobIdSupplier, "jobIdSupplier must not be null");
        timeSupplier = Objects.requireNonNull(builder.timeSupplier, "timeSupplier must not be null");
    }

    public static Builder<IngestJob> forIngestJob() {
        return builder()
                .deserialiser(new IngestJobSerDe()::fromJson)
                .toIngestJob(job -> job)
                .applyIngestJobChanges((job, changedJob) -> changedJob);
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    public Optional<T> deserialiseAndValidate(String message) {
        T job;
        try {
            job = deserialiser.apply(message);
            LOGGER.info("Deserialised message to ingest job {}", job);
        } catch (RuntimeException e) {
            LOGGER.warn("Deserialisation failed for message: {}", message, e);
            ingestJobStatusStore.jobValidated(
                    ingestJobRejected(jobIdSupplier.get(), message, timeSupplier.get(),
                            "Error parsing JSON. Reason: " + Optional.ofNullable(e.getCause()).orElse(e).getMessage()));
            return Optional.empty();
        }
        IngestJob ingestJob = toIngestJob.apply(job);
        String jobId = ingestJob.getId();
        if (jobId == null || jobId.isBlank()) {
            jobId = jobIdSupplier.get();
            LOGGER.info("Null or blank id provided. Generated new id: {}", jobId);
        }

        List<String> files = ingestJob.getFiles();
        List<String> validationFailures = new ArrayList<>();
        if (files == null) {
            validationFailures.add("Missing property \"files\"");
        } else if (files.contains(null)) {
            validationFailures.add("One of the files was null");
        }
        Optional<TableStatus> tableOpt = getTable(ingestJob);
        if (tableOpt.isEmpty()) {
            validationFailures.add("Table not found");
        }
        if (!validationFailures.isEmpty()) {
            LOGGER.warn("Validation failed: {}", validationFailures);
            ingestJobStatusStore.jobValidated(
                    refusedEventBuilder()
                            .jobId(jobId)
                            .tableId(tableOpt.map(TableStatus::getTableUniqueId).orElse(null))
                            .jsonMessage(message)
                            .reasons(validationFailures)
                            .build());
            return Optional.empty();
        }
        TableStatus table = tableOpt.get();

        List<String> expandedFiles = expandDirectories.apply(files);
        if (expandedFiles.isEmpty()) {
            LOGGER.warn("Could not find one or more files for job: {}", job);
            ingestJobStatusStore.jobValidated(
                    refusedEventBuilder()
                            .jobId(jobId)
                            .tableId(table.getTableUniqueId())
                            .jsonMessage(message)
                            .reasons("Could not find one or more files")
                            .build());
            return Optional.empty();
        }

        LOGGER.info("No validation failures found");
        return Optional.of(applyIngestJobChanges.apply(job,
                ingestJob.toBuilder()
                        .id(jobId)
                        .tableName(table.getTableName())
                        .tableId(table.getTableUniqueId())
                        .files(expandedFiles)
                        .build()));
    }

    private Optional<TableStatus> getTable(IngestJob job) {
        if (job.getTableId() != null) {
            return tableIndex.getTableByUniqueId(job.getTableId());
        } else if (job.getTableName() != null) {
            return tableIndex.getTableByName(job.getTableName());
        } else {
            return Optional.empty();
        }
    }

    private IngestJobValidatedEvent.Builder refusedEventBuilder() {
        return IngestJobValidatedEvent.builder()
                .validationTime(timeSupplier.get());
    }

    public static final class Builder<T> {
        private TableIndex tableIndex;
        private IngestJobStatusStore ingestJobStatusStore;
        private Function<String, T> deserialiser;
        private Function<T, IngestJob> toIngestJob;
        private BiFunction<T, IngestJob, T> applyIngestJobChanges;
        private Function<List<String>, List<String>> expandDirectories;
        private Supplier<String> jobIdSupplier = () -> UUID.randomUUID().toString();
        private Supplier<Instant> timeSupplier = Instant::now;

        private Builder() {
        }

        public Builder<T> tableIndex(TableIndex tableIndex) {
            this.tableIndex = tableIndex;
            return this;
        }

        public Builder<T> ingestJobStatusStore(IngestJobStatusStore ingestJobStatusStore) {
            this.ingestJobStatusStore = ingestJobStatusStore;
            return this;
        }

        public <N> Builder<N> deserialiser(Function<String, N> deserialiser) {
            this.deserialiser = (Function<String, T>) deserialiser;
            return (Builder<N>) this;
        }

        public Builder<T> toIngestJob(Function<T, IngestJob> toIngestJob) {
            this.toIngestJob = toIngestJob;
            return this;
        }

        public Builder<T> applyIngestJobChanges(BiFunction<T, IngestJob, T> applyIngestJobChanges) {
            this.applyIngestJobChanges = applyIngestJobChanges;
            return this;
        }

        public Builder<T> expandDirectories(Function<List<String>, List<String>> expandDirectories) {
            this.expandDirectories = expandDirectories;
            return this;
        }

        public Builder<T> jobIdSupplier(Supplier<String> jobIdSupplier) {
            this.jobIdSupplier = jobIdSupplier;
            return this;
        }

        public Builder<T> timeSupplier(Supplier<Instant> timeSupplier) {
            this.timeSupplier = timeSupplier;
            return this;
        }

        public IngestJobMessageHandler<T> build() {
            return new IngestJobMessageHandler<>(this);
        }
    }
}
