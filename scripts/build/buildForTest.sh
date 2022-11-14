
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

BASE_DIR=$(cd "$(dirname "$0")" && cd "../../" && pwd)
MAVEN_DIR="$BASE_DIR/java"
SCRIPTS_DIR="$BASE_DIR/scripts"
JARS_DIR="$SCRIPTS_DIR/jars"
DOCKER_DIR="$SCRIPTS_DIR/docker"

"$SCRIPTS_DIR/build/build.sh"

VERSION=$(cat "$TEMPLATE_DIR/version.txt")

cp -r "$MAVEN_DIR/system-test/docker" "$DOCKER_DIR/system-test"
cp -r "$MAVEN_DIR/system-test/target/system-test-${VERSION}-utility.jar" "$DOCKER_DIR/system-test/system-test.jar"
cp -r "$MAVEN_DIR/system-test/target/system-test-${VERSION}-utility.jar" "$JARS_DIR"
