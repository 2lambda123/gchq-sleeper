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
package sleeper.query.recordretrieval;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.query.QueryException;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.query.utils.RangeQueryUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Executes a {@link LeafPartitionQuery}.
 */
public class LeafPartitionQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeafPartitionQueryExecutor.class);

    private final ExecutorService executorService;
    private final ObjectFactory objectFactory;
    private final Configuration conf;
    private final TableProperties tableProperties;

    public LeafPartitionQueryExecutor(
            ExecutorService executorService,
            ObjectFactory objectFactory,
            Configuration conf,
            TableProperties tableProperties) {
        this.executorService = executorService;
        this.objectFactory = objectFactory;
        this.conf = conf;
        this.tableProperties = tableProperties;
    }

    public CloseableIterator<Record> getRecords(LeafPartitionQuery leafPartitionQuery) throws QueryException {
        LOGGER.info("Retrieving records for LeafPartitionQuery {}", leafPartitionQuery);
        List<String> files = leafPartitionQuery.getFiles();
        Schema tableSchema = tableProperties.getSchema();
        String compactionIteratorClassName = tableProperties.get(TableProperty.ITERATOR_CLASS_NAME);
        String compactionIteratorConfig = tableProperties.get(TableProperty.ITERATOR_CONFIG);
        SortedRecordIterator compactionIterator;
        SortedRecordIterator queryIterator;

        try {
            compactionIterator = createIterator(tableSchema, objectFactory, compactionIteratorClassName, compactionIteratorConfig);
            queryIterator = createIterator(tableSchema, objectFactory, leafPartitionQuery.getQueryTimeIteratorClassName(), leafPartitionQuery.getQueryTimeIteratorConfig());
        } catch (IteratorException e) {
            throw new QueryException("Failed to initialise iterators", e);
        }

        Schema dataReadSchema = createSchemaForDataRead(leafPartitionQuery, tableSchema, compactionIterator, queryIterator);

        FilterPredicate filterPredicate = RangeQueryUtils.getFilterPredicateMultidimensionalKey
                (tableSchema.getRowKeyFields(), leafPartitionQuery.getRegions(), leafPartitionQuery.getPartitionRegion());

        LeafPartitionRecordRetriever retriever = new LeafPartitionRecordRetriever(executorService, conf);

        try {
            CloseableIterator<Record> iterator = retriever.getRecords(files, dataReadSchema, filterPredicate);
            // Apply compaction time iterator
            if (null != compactionIterator) {
                iterator = compactionIterator.apply(iterator);
            }
            // Apply query time iterator
            if (null != queryIterator) {
                iterator = queryIterator.apply(iterator);
            }

            return iterator;
        } catch (RecordRetrievalException e) {
            throw new QueryException("QueryException retrieving records for LeafPartitionQuery", e);
        }
    }

    private Schema createSchemaForDataRead(Query query, Schema schema, SortedRecordIterator compactionIterator, SortedRecordIterator queryIterator) {
        List<String> requestedValueFields = query.getRequestedValueFields();
        if (requestedValueFields == null) {
            return schema;
        }

        Map<String, Field> fields = new HashMap<>();
        schema.getValueFields().forEach(field -> fields.put(field.getName(), field));

        Set<String> requiredFields = new HashSet<>(requestedValueFields);

        if (compactionIterator != null) {
            requiredFields.addAll(compactionIterator.getRequiredValueFields());
        }
        if (queryIterator != null) {
            requiredFields.addAll(queryIterator.getRequiredValueFields());
        }

        return Schema.builder()
                .rowKeyFields(schema.getRowKeyFields())
                .sortKeyFields(schema.getSortKeyFields())
                .valueFields(requiredFields.stream()
                        .map(fields::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()))
                .build();
    }

    private SortedRecordIterator createIterator(
            Schema schema,
            ObjectFactory objectFactory,
            String iteratorClassName,
            String iteratorConfig) throws IteratorException {
        if (iteratorClassName == null) {
            return null;
        }
        SortedRecordIterator sortedRecordIterator;
        try {
            sortedRecordIterator = objectFactory.getObject(iteratorClassName, SortedRecordIterator.class);
        } catch (ObjectFactoryException e) {
            throw new IteratorException("ObjectFactoryException creating iterator of class " + iteratorClassName, e);
        }
        LOGGER.debug("Created iterator of class {}", iteratorClassName);
        sortedRecordIterator.init(iteratorConfig, schema);
        LOGGER.debug("Initialised iterator with config " + iteratorConfig);

        return sortedRecordIterator;
    }
}
