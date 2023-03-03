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
package sleeper.cdk.jars;

import sleeper.core.SleeperVersion;

public class BuiltJarNew {

    public static final BuiltJarNew ATHENA = fromFormat("athena-%s.jar");
    public static final BuiltJarNew BULK_IMPORT_STARTER = fromFormat("bulk-import-starter-%s.jar");
    public static final BuiltJarNew INGEST_STARTER = fromFormat("ingest-starter-%s.jar");
    public static final BuiltJarNew GARBAGE_COLLECTOR = fromFormat("lambda-garbagecollector-%s.jar");
    public static final BuiltJarNew COMPACTION_JOB_CREATOR = fromFormat("lambda-jobSpecCreationLambda-%s.jar");
    public static final BuiltJarNew COMPACTION_TASK_CREATOR = fromFormat("runningjobs-%s.jar");
    public static final BuiltJarNew PARTITION_SPLITTER = fromFormat("lambda-splitter-%s.jar");
    public static final BuiltJarNew QUERY = fromFormat("query-%s.jar");
    public static final BuiltJarNew CUSTOM_RESOURCES = fromFormat("cdk-custom-resources-%s.jar");
    public static final BuiltJarNew METRICS = fromFormat("metrics-%s.jar");

    private final String fileName;

    private BuiltJarNew(String fileName) {
        this.fileName = fileName;
    }

    public static BuiltJarNew fromFormat(String format) {
        return new BuiltJarNew(String.format(format, SleeperVersion.getVersion()));
    }

    public String getFileName() {
        return fileName;
    }
}
