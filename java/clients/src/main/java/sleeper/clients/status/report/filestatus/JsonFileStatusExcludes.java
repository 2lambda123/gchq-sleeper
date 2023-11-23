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
package sleeper.clients.status.report.filestatus;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

import sleeper.core.statestore.FileInfo;

import java.util.List;

public class JsonFileStatusExcludes implements ExclusionStrategy {
    private static final List<String> EXCLUDED_FIELDS = List.of("countApproximate", "onlyContainsDataForThisPartition");

    @Override
    public boolean shouldSkipField(FieldAttributes f) {
        if (FileInfo.class == f.getDeclaringClass() && EXCLUDED_FIELDS.contains(f.getName())) {
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldSkipClass(Class<?> clazz) {
        return false;
    }
}
