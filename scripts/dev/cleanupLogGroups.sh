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
unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$(dirname "${THIS_DIR}")")

pushd "${PROJECT_ROOT}/java"
echo "Compiling..."
mvn compile -Pquick -q -pl clients -am

pushd clients
echo "Cleaning up log groups..."
mvn exec:java -q \
  -Dexec.mainClass="sleeper.clients.status.update.CleanUpLogGroups" \
  -Dexec.args="$PROJECT_ROOT"

popd
popd