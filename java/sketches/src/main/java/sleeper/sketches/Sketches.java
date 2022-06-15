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
package sleeper.sketches;

import org.apache.datasketches.quantiles.ItemsSketch;

import java.util.Map;

public class Sketches {
    private final Map<String, ItemsSketch> keyFieldToQuantilesSketch;

    public Sketches(Map<String, ItemsSketch> keyFieldToQuantilesSketch) {
        this.keyFieldToQuantilesSketch = keyFieldToQuantilesSketch;
    }

    public Map<String, ItemsSketch> getQuantilesSketches() {
        return keyFieldToQuantilesSketch;
    }

    public ItemsSketch getQuantilesSketch(String keyFieldName) {
        return keyFieldToQuantilesSketch.get(keyFieldName);
    }
}
