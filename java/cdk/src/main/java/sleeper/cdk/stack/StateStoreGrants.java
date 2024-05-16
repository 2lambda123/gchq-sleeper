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
package sleeper.cdk.stack;

public class StateStoreGrants {
    public enum Access {
        NO_ACCESS,
        READ,
        READ_WRITE
    }

    private final Access activeFiles;
    private final Access readyForGCFiles;
    private final Access partitions;

    public static Builder builder() {
        return new Builder();
    }

    private StateStoreGrants(Builder builder) {
        this.activeFiles = builder.activeFiles;
        this.readyForGCFiles = builder.readyForGCFiles;
        this.partitions = builder.partitions;
    }

    public static class Builder {
        private Access activeFiles = Access.NO_ACCESS;
        private Access readyForGCFiles = Access.NO_ACCESS;
        private Access partitions = Access.NO_ACCESS;

        public Builder activeFiles(Access activeFiles) {
            this.activeFiles = activeFiles;
            return this;
        }

        public Builder readyForGCFiles(Access readyForGCFiles) {
            this.readyForGCFiles = readyForGCFiles;
            return this;
        }

        public Builder partitions(Access partitions) {
            this.partitions = partitions;
            return this;
        }

        public StateStoreGrants build() {
            return new StateStoreGrants(this);
        }
    }
}
