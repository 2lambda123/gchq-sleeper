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

package sleeper.clients.admin;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropertiesDiff {
    private final List<PropertyDiff> propertyDiffs;

    public PropertiesDiff(Map<String, String> before, Map<String, String> after) {
        this.propertyDiffs = calculateDiffs(before, after);
    }

    public List<PropertyDiff> getChanges() {
        return propertyDiffs;
    }

    private static List<PropertyDiff> calculateDiffs(
            Map<String, String> before, Map<String, String> after) {
        return getPropertyDiffs(before, after).collect(Collectors.toList());
    }

    private static Stream<PropertyDiff> getPropertyDiffs(Map<String, String> before, Map<String, String> after) {
        return getAllSetPropertyNames(before, after)
                .flatMap(propertyName -> PropertyDiff.compare(propertyName, before, after).stream());
    }

    private static Stream<String> getAllSetPropertyNames(Map<String, String> before, Map<String, String> after) {
        Set<String> propertyNames = new HashSet<>();
        propertyNames.addAll(before.keySet());
        propertyNames.addAll(after.keySet());
        return propertyNames.stream();
    }
}
