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

package sleeper.systemtest.suite.dsl.query;

import sleeper.core.record.Record;
import sleeper.systemtest.drivers.query.DirectQueryDriver;
import sleeper.systemtest.drivers.query.QueryAllTablesDriver;
import sleeper.systemtest.drivers.query.QueryAllTablesInParallelDriver;
import sleeper.systemtest.drivers.query.QueryAllTablesSendAndWaitDriver;
import sleeper.systemtest.drivers.query.QueryCreator;
import sleeper.systemtest.drivers.query.QueryRange;
import sleeper.systemtest.drivers.query.S3ResultsDriver;
import sleeper.systemtest.drivers.query.SQSQueryDriver;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;

import java.util.List;
import java.util.Map;

public class SystemTestQuery {
    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;
    private QueryAllTablesDriver driver = null;

    public SystemTestQuery(SleeperInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.clients = clients;
    }

    public SystemTestQuery byQueue() {
        driver = new QueryAllTablesSendAndWaitDriver(instance,
                new SQSQueryDriver(instance, clients.getSqs(), clients.getDynamoDB(), clients.getS3()));
        return this;
    }

    public SystemTestQuery direct() {
        driver = new QueryAllTablesInParallelDriver(instance, new DirectQueryDriver(instance));
        return this;
    }

    public List<Record> allRecordsInTable() {
        return driver.run(queryCreator().allRecordsQuery());
    }

    public Map<String, List<Record>> allRecordsByTable() {
        return driver.runForAllTables(QueryCreator::allRecordsQuery);
    }

    public List<Record> byRowKey(String key, QueryRange... ranges) {
        return driver.run(queryCreator().byRowKey(key, List.of(ranges)));
    }

    public void emptyResultsBucket() {
        new S3ResultsDriver(instance, clients.getS3()).emptyBucket();
    }

    private QueryCreator queryCreator() {
        return new QueryCreator(instance);
    }
}
