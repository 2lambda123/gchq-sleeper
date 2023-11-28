#!/usr/bin/env bash
#
# Copyright 2022-2023 Crown Copyright
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
#

set -e
unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
MAVEN_DIR=$(cd "$SCRIPTS_DIR" && cd ../java && pwd)

pushd "$SCRIPTS_DIR/test"

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <vpc> <subnet> <results-bucket> <test-type>"
  echo "Valid test types are: performance, functional"
  exit 1
fi

VPC=$1
SUBNETS=$2
RESULTS_BUCKET=$3
if [ "$4" == "performance" ]; then
  TEST_SUITE_PARAMS="-Dsleeper.system.test.cluster.enabled=true"
  TEST_SUITE_NAME="performance"
elif [ "$4" == "functional" ]; then
  TEST_SUITE_PARAMS="-Dsleeper.system.test.cluster.enabled=false"
  TEST_SUITE_NAME="functional"
else
  echo "Invalid test type: $4"
  echo "Valid test types are: performance, functional"
  exit 1
fi
source "$SCRIPTS_DIR/functions/timeUtils.sh"
source "$SCRIPTS_DIR/functions/systemTestUtils.sh"
START_TIMESTAMP=$(record_time)
START_TIME=$(recorded_time_str "$START_TIMESTAMP" "%Y%m%d-%H%M%S")
START_TIME_SHORT=$(recorded_time_str "$START_TIMESTAMP" "%m%d%H%M")
OUTPUT_DIR="/tmp/sleeper/${TEST_SUITE_NAME}Tests/$START_TIME"

mkdir -p "$OUTPUT_DIR"
../build/buildForTest.sh
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
SYSTEM_TEST_JAR="$SCRIPTS_DIR/jars/system-test-${VERSION}-utility.jar"
set +e

END_EXIT_CODE=0

runMavenSystemTests() {
    SHORT_ID=$1
    TEST_NAME=$2
    EXTRA_MAVEN_PARAMS=$3
    TEST_OUTPUT_DIR="$OUTPUT_DIR/$TEST_NAME"
    mkdir "$TEST_OUTPUT_DIR"
    ./maven/deployTest.sh "$SHORT_ID" "$VPC" "$SUBNETS" \
      -Dsleeper.system.test.output.dir="$TEST_OUTPUT_DIR" \
      "$EXTRA_MAVEN_PARAMS" \
      &> "$OUTPUT_DIR/$TEST_NAME.log"
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
      END_EXIT_CODE=$EXIT_CODE
    fi
    echo -n "$EXIT_CODE $SHORT_ID" > "$OUTPUT_DIR/$TEST_NAME.status"
    pushd "$MAVEN_DIR"
    mvn --batch-mode site site:stage -pl system-test/system-test-suite \
       -DskipTests=true \
       -DstagingDirectory="$TEST_OUTPUT_DIR/site"
    popd
    pushd "$TEST_OUTPUT_DIR/site"
    zip -r "$OUTPUT_DIR/$TEST_NAME-site.zip" "."
    popd
    rm -rf "$TEST_OUTPUT_DIR/site"
    INSTANCE_IDS=()
    read_instance_ids_to_array "$TEST_OUTPUT_DIR/instanceIds.txt" INSTANCE_IDS
    ./maven/tearDown.sh "$SHORT_ID" "${INSTANCE_IDS[@]}" &> "$OUTPUT_DIR/$TEST_NAME.tearDown.log"
}

runMavenSystemTests "mvn-$START_TIME_SHORT" $TEST_SUITE_NAME $TEST_SUITE_PARAMS
runMavenSystemTests "dyn-$START_TIME_SHORT" dynamo-state-store -Dsleeper.system.test.force.statestore.classname=sleeper.statestore.dynamodb.DynamoDBStateStore

echo "[$(time_str)] Uploading test output"
java -cp "${SYSTEM_TEST_JAR}" \
 sleeper.systemtest.drivers.nightly.RecordNightlyTestOutput "$RESULTS_BUCKET" "$START_TIMESTAMP" "$OUTPUT_DIR"

popd

exit $END_EXIT_CODE
