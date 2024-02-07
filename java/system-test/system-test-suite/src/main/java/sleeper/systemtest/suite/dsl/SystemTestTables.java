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

package sleeper.systemtest.suite.dsl;

import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableIdentity;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;

import java.util.List;
import java.util.Map;

public class SystemTestTables {

    private final SleeperInstanceContext instance;

    public SystemTestTables(SleeperInstanceContext instance) {
        this.instance = instance;
    }

    public void createMany(int numberOfTables, Schema schema) {
        createManyWithProperties(numberOfTables, schema, Map.of());
    }

    public void createManyWithProperties(int numberOfTables, Schema schema, Map<TableProperty, String> setProperties) {
        instance.createTables(numberOfTables, schema, setProperties);
    }

    public List<TableIdentity> loadIdentities() {
        return instance.loadTableIdentities();
    }
}
