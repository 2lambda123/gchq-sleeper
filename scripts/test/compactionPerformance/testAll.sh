#!/usr/bin/env bash
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

set -e

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)

source "$SCRIPTS_DIR/functions/timeUtils.sh"
START_TIME=$(record_time)

"$THIS_DIR/testIngest.sh"

END_INGEST=$(record_time)

"$THIS_DIR/testCompaction.sh"

END_COMPACTION=$(record_time)

"$THIS_DIR/testSplittingCompaction.sh"

FINISH_TIME=$(record_time)
echo "-------------------------------------------------------------------------------"
echo "Finished compaction performance test"
echo "-------------------------------------------------------------------------------"
echo "Started at $(recorded_time_str "$START_TIME")"
echo "Ingest finished at $(recorded_time_str "$END_INGEST"), took $(elapsed_time_str "$START_TIME" "$END_INGEST")"
echo "Compaction finished at $(recorded_time_str "$END_COMPACTION"), took $(elapsed_time_str "$END_INGEST" "$END_COMPACTION")"
echo "Splitting compaction finished at $(recorded_time_str "$FINISH_TIME"), took $(elapsed_time_str "$END_COMPACTION" "$FINISH_TIME")"
echo "Overall, tests took $(elapsed_time_str "$START_TIME" "$FINISH_TIME")"
