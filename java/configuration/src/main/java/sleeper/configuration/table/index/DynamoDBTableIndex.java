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

package sleeper.configuration.table.index;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CancellationReason;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableAlreadyOfflineException;
import sleeper.core.table.TableAlreadyOnlineException;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_INDEX_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.table.index.DynamoDBTableIdFormat.ONLINE_FIELD;
import static sleeper.configuration.table.index.DynamoDBTableIdFormat.getIdKey;
import static sleeper.configuration.table.index.DynamoDBTableIdFormat.getNameKey;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createBooleanAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBTableIndex implements TableIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTableIndex.class);

    public static final String TABLE_NAME_FIELD = DynamoDBTableIdFormat.TABLE_NAME_FIELD;
    public static final String TABLE_ID_FIELD = DynamoDBTableIdFormat.TABLE_ID_FIELD;

    private final AmazonDynamoDB dynamoDB;
    private final String nameIndexDynamoTableName;
    private final String idIndexDynamoTableName;
    private final boolean stronglyConsistent;

    public DynamoDBTableIndex(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
        this.nameIndexDynamoTableName = instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME);
        this.idIndexDynamoTableName = instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME);
        this.stronglyConsistent = instanceProperties.getBoolean(TABLE_INDEX_DYNAMO_STRONGLY_CONSISTENT_READS);
    }

    @Override
    public void create(TableIdentity tableId) throws TableAlreadyExistsException {
        Map<String, AttributeValue> idItem = DynamoDBTableIdFormat.online(tableId);
        TransactWriteItemsRequest request = new TransactWriteItemsRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTransactItems(
                        new TransactWriteItem().withPut(new Put()
                                .withTableName(nameIndexDynamoTableName)
                                .withItem(idItem)
                                .withConditionExpression("attribute_not_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))),
                        new TransactWriteItem().withPut(new Put()
                                .withTableName(idIndexDynamoTableName)
                                .withItem(idItem)
                                .withConditionExpression("attribute_not_exists(#tableid)")
                                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Created table {}, capacity consumed = {}", tableId, totalCapacity);
        } catch (TransactionCanceledException e) {
            CancellationReason nameIndexReason = e.getCancellationReasons().get(0);
            if (isCheckFailed(nameIndexReason)) {
                throw new TableAlreadyExistsException(tableId);
            } else {
                throw e;
            }
        }
    }

    @Override
    public Stream<TableIdentity> streamAllTables() {
        return streamPagedItems(dynamoDB,
                new ScanRequest()
                        .withTableName(nameIndexDynamoTableName)
                        .withConsistentRead(stronglyConsistent))
                .map(DynamoDBTableIdFormat::readItem)
                .sorted(Comparator.comparing(TableIdentity::getTableName));
    }

    @Override
    public Stream<TableIdentity> streamOnlineTables() {
        return streamPagedItems(dynamoDB,
                new ScanRequest()
                        .withTableName(nameIndexDynamoTableName)
                        .withConsistentRead(stronglyConsistent)
                        .withFilterExpression("#Online = :online")
                        .withExpressionAttributeNames(Map.of("#Online", ONLINE_FIELD))
                        .withExpressionAttributeValues(Map.of(":online", createBooleanAttribute(true))))
                .map(DynamoDBTableIdFormat::readItem)
                .sorted(Comparator.comparing(TableIdentity::getTableName));
    }

    @Override
    public Optional<TableIdentity> getTableByName(String tableName) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(nameIndexDynamoTableName)
                .withConsistentRead(stronglyConsistent)
                .addKeyConditionsEntry(TABLE_NAME_FIELD, new Condition()
                        .withAttributeValueList(createStringAttribute(tableName))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return result.getItems().stream().map(DynamoDBTableIdFormat::readItem).findFirst();
    }

    @Override
    public Optional<TableIdentity> getTableByUniqueId(String tableUniqueId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(idIndexDynamoTableName)
                .withConsistentRead(stronglyConsistent)
                .addKeyConditionsEntry(TABLE_ID_FIELD, new Condition()
                        .withAttributeValueList(createStringAttribute(tableUniqueId))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return result.getItems().stream().map(DynamoDBTableIdFormat::readItem).findFirst();
    }

    @Override
    public void delete(TableIdentity tableId) {
        TransactWriteItemsRequest request = new TransactWriteItemsRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTransactItems(
                        new TransactWriteItem().withDelete(new Delete()
                                .withTableName(nameIndexDynamoTableName)
                                .withKey(DynamoDBTableIdFormat.getNameKey(tableId))
                                .withConditionExpression("attribute_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))),
                        new TransactWriteItem().withDelete(new Delete()
                                .withTableName(idIndexDynamoTableName)
                                .withKey(DynamoDBTableIdFormat.getIdKey(tableId))
                                .withConditionExpression("attribute_exists(#tableid)")
                                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Deleted table {}, capacity consumed = {}", tableId, totalCapacity);
        } catch (TransactionCanceledException e) {
            CancellationReason nameNotFoundReason = e.getCancellationReasons().get(0);
            CancellationReason idNotFoundReason = e.getCancellationReasons().get(1);
            if (isCheckFailed(nameNotFoundReason)) {
                throw TableNotFoundException.withTableName(tableId.getTableName());
            } else if (isCheckFailed(idNotFoundReason)) {
                throw TableNotFoundException.withTableId(tableId.getTableUniqueId());
            } else {
                throw e;
            }
        }
    }

    @Override
    public void update(TableIdentity tableId) {
        TableIdentity oldId = getTableByUniqueId(tableId.getTableUniqueId())
                .orElseThrow(() -> TableNotFoundException.withTableId(tableId.getTableUniqueId()));
        update(oldId, tableId);
    }

    public void update(TableIdentity oldId, TableIdentity newId) {
        Map<String, AttributeValue> idItem = DynamoDBTableIdFormat.getItem(newId);
        TransactWriteItemsRequest request = new TransactWriteItemsRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTransactItems(
                        new TransactWriteItem().withPut(new Put()
                                .withTableName(nameIndexDynamoTableName)
                                .withItem(idItem)
                                .withConditionExpression("attribute_not_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))),
                        new TransactWriteItem().withPut(new Put()
                                .withTableName(idIndexDynamoTableName)
                                .withItem(idItem)
                                .withConditionExpression("attribute_exists(#tableid)")
                                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))),
                        new TransactWriteItem().withDelete(new Delete()
                                .withTableName(nameIndexDynamoTableName)
                                .withKey(DynamoDBTableIdFormat.getNameKey(oldId))
                                .withConditionExpression("attribute_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Updated table {}, capacity consumed = {}", newId, totalCapacity);
        } catch (TransactionCanceledException e) {
            CancellationReason nameAlreadyExistsReason = e.getCancellationReasons().get(0);
            CancellationReason idNotFoundReason = e.getCancellationReasons().get(1);
            CancellationReason oldNameNotFoundReason = e.getCancellationReasons().get(2);
            if (isCheckFailed(nameAlreadyExistsReason)) {
                throw new TableAlreadyExistsException(getTableByName(newId.getTableName())
                        .orElseThrow(() -> TableNotFoundException.withTableName(newId.getTableName())));
            } else if (isCheckFailed(idNotFoundReason)) {
                throw TableNotFoundException.withTableId(newId.getTableUniqueId());
            } else if (isCheckFailed(oldNameNotFoundReason)) {
                throw TableNotFoundException.withTableName(oldId.getTableName());
            } else {
                throw e;
            }
        }
    }

    @Override
    public void takeOffline(TableIdentity tableId) {
        setOnline(tableId, false);
    }

    @Override
    public void putOnline(TableIdentity tableId) {
        setOnline(tableId, true);
    }

    private void setOnline(TableIdentity tableId, boolean online) {
        TransactWriteItemsRequest request = new TransactWriteItemsRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTransactItems(
                        new TransactWriteItem().withUpdate(new Update()
                                .withTableName(nameIndexDynamoTableName)
                                .withKey(getNameKey(tableId))
                                .withUpdateExpression("SET #Online = :after")
                                .withConditionExpression("#Online = :before")
                                .withExpressionAttributeNames(Map.of("#Online", ONLINE_FIELD))
                                .withExpressionAttributeValues(Map.of(
                                        ":before", createBooleanAttribute(!online),
                                        ":after", createBooleanAttribute(online)))
                                .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)),
                        new TransactWriteItem().withUpdate(new Update()
                                .withTableName(idIndexDynamoTableName)
                                .withKey(getIdKey(tableId))
                                .withUpdateExpression("SET #Online = :after")
                                .withConditionExpression("#Online = :before")
                                .withExpressionAttributeNames(Map.of("#Online", ONLINE_FIELD))
                                .withExpressionAttributeValues(Map.of(
                                        ":before", createBooleanAttribute(!online),
                                        ":after", createBooleanAttribute(online)))
                                .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Created table {}, capacity consumed = {}", tableId, totalCapacity);
        } catch (TransactionCanceledException e) {
            CancellationReason nameIndexReason = e.getCancellationReasons().get(0);
            CancellationReason idIndexReason = e.getCancellationReasons().get(1);
            if (isCheckFailed(nameIndexReason)) {
                if (nameIndexReason.getItem() == null) {
                    throw TableNotFoundException.withTableName(tableId.getTableName(), e);
                } else {
                    throw getOnlineException(tableId, online, e);
                }
            } else if (isCheckFailed(idIndexReason)) {
                if (idIndexReason.getItem() == null) {
                    throw TableNotFoundException.withTableId(tableId.getTableUniqueId(), e);
                } else {
                    throw getOnlineException(tableId, online, e);
                }
            } else {
                throw e;
            }
        }
    }

    private RuntimeException getOnlineException(TableIdentity tableId, boolean online, Exception cause) {
        if (online) {
            return new TableAlreadyOnlineException(tableId, cause);
        } else {
            return new TableAlreadyOfflineException(tableId, cause);
        }
    }

    private static boolean isCheckFailed(CancellationReason reason) {
        return "ConditionalCheckFailed".equals(reason.getCode());
    }
}
