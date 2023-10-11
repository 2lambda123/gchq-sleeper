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

package sleeper.configuration.properties.table;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.instance.SleeperProperty;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

class TablePropertyImpl implements TableProperty {

    private final String propertyName;
    private final String defaultValue;
    private final Predicate<String> validationPredicate;
    private final SleeperProperty defaultProperty;
    private final String description;
    private final PropertyGroup propertyGroup;
    private final boolean editable;
    private final boolean includedInTemplate;

    private TablePropertyImpl(Builder builder) {
        propertyName = Objects.requireNonNull(builder.propertyName, "propertyName must not be null");
        defaultValue = builder.defaultValue;
        validationPredicate = Objects.requireNonNull(builder.validationPredicate, "validationPredicate must not be null");
        defaultProperty = builder.defaultProperty;
        description = Objects.requireNonNull(builder.description, "description must not be null");
        propertyGroup = Objects.requireNonNull(builder.propertyGroup, "propertyGroup must not be null");
        editable = builder.editable;
        includedInTemplate = builder.includedInTemplate;
    }

    static Builder builder() {
        return new Builder();
    }

    public static Builder named(String name) {
        return builder().propertyName(name);
    }

    @Override
    public Predicate<String> validationPredicate() {
        return validationPredicate;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public SleeperProperty getDefaultProperty() {
        return defaultProperty;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public PropertyGroup getPropertyGroup() {
        return propertyGroup;
    }

    @Override
    public boolean isRunCdkDeployWhenChanged() {
        return false;
    }

    @Override
    public boolean isEditable() {
        return editable;
    }

    @Override
    public boolean isIncludedInTemplate() {
        return includedInTemplate;
    }

    public String toString() {
        return propertyName;
    }

    static final class Builder {
        private String propertyName;
        private String defaultValue;
        private Predicate<String> validationPredicate = s -> true;
        private SleeperProperty defaultProperty;
        private String description;
        private PropertyGroup propertyGroup;
        private Consumer<TableProperty> addToIndex;
        private boolean editable = true;
        private boolean includedInTemplate = true;

        private Builder() {
        }

        public Builder propertyName(String propertyName) {
            this.propertyName = propertyName;
            return this;
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder validationPredicate(Predicate<String> validationPredicate) {
            this.validationPredicate = validationPredicate;
            return this;
        }

        public Builder defaultProperty(SleeperProperty defaultProperty) {
            this.defaultProperty = defaultProperty;
            this.defaultValue = defaultProperty.getDefaultValue();
            return validationPredicate(defaultProperty.validationPredicate());
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder editable(boolean editable) {
            this.editable = editable;
            return this;
        }

        public Builder includedInTemplate(boolean includedInTemplate) {
            this.includedInTemplate = includedInTemplate;
            return this;
        }

        public Builder propertyGroup(PropertyGroup propertyGroup) {
            this.propertyGroup = propertyGroup;
            return this;
        }

        public Builder addToIndex(Consumer<TableProperty> addToIndex) {
            this.addToIndex = addToIndex;
            return this;
        }

        // We want an exception to be thrown if addToIndex is null
        @SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
        public TableProperty build() {
            TableProperty property = new TablePropertyImpl(this);
            addToIndex.accept(property);
            return property;
        }
    }
}
