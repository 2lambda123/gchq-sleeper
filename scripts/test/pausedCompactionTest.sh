#!/usr/bin/env bash
# Copyright 2022 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

TABLE_NAME="system-test"
SCRIPTS_DIR=$(cd "$(dirname "$0")" && cd .. && pwd)
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
JARS_DIR="$SCRIPTS_DIR/jars"
GENERATED_DIR="$SCRIPTS_DIR/generated"

SYSTEM_TEST_JAR="$JARS_DIR/system-test-$VERSION-utility.jar"

INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
INSTANCE_ID=$(grep -F sleeper.id "${INSTANCE_PROPERTIES}" | cut -d'=' -f2)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

"$SCRIPTS_DIR/test/waitForIngest.sh"
END_WAIT_FOR_INGEST_TIME=$(record_time)

echo "-------------------------------------------------------------------------------"
echo "Triggering compaction job creation"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.compaction.TriggerCompactionJobCreation "$INSTANCE_ID"

END_TRIGGER_COMPACTION_JOBS=$(record_time)
echo "Triggering compaction jobs finished at $(recorded_time_str "$END_TRIGGER_COMPACTION_JOBS"), took $(elapsed_time_str "$END_WAIT_FOR_INGEST_TIME" "$END_TRIGGER_COMPACTION_JOBS")"

echo "-------------------------------------------------------------------------------"
echo "Triggering compaction task creation"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.compaction.TriggerCompactionTaskCreation "$INSTANCE_ID"

END_TRIGGER_COMPACTION_TASKS=$(record_time)
echo "Triggering compaction tasks finished at $(recorded_time_str "$END_TRIGGER_COMPACTION_TASKS"), took $(elapsed_time_str "$END_TRIGGER_COMPACTION_JOBS" "$END_TRIGGER_COMPACTION_TASKS")"

echo "-------------------------------------------------------------------------------"
echo "Waiting for compaction jobs"
echo "-------------------------------------------------------------------------------"
java -cp "${SYSTEM_TEST_JAR}" \
sleeper.systemtest.compaction.WaitForCompactionJobs "$INSTANCE_ID" "$TABLE_NAME"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished compaction test"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Waiting for ingest finished at $(recorded_time_str "$END_WAIT_FOR_INGEST_TIME"), took $(elapsed_time_str "$START_TIME" "$END_WAIT_FOR_INGEST_TIME")"
echo "Triggering compaction jobs finished at $(recorded_time_str "$END_TRIGGER_COMPACTION_JOBS"), took $(elapsed_time_str "$END_WAIT_FOR_INGEST_TIME" "$END_TRIGGER_COMPACTION_JOBS")"
echo "Triggering compaction tasks finished at $(recorded_time_str "$END_TRIGGER_COMPACTION_TASKS"), took $(elapsed_time_str "$END_TRIGGER_COMPACTION_JOBS" "$END_TRIGGER_COMPACTION_TASKS")"
echo "Compaction finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_TRIGGER_COMPACTION_TASKS" "$FINISH_TIME")"
echo "Overall, took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
