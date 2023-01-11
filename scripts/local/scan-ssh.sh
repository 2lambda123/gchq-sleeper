
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
ENVIRONMENTS_DIR="$THIS_DIR/environments"
INSTANCE_ID=$(cat "$ENVIRONMENTS_DIR/current.txt")
OUTPUTS_FILE="$ENVIRONMENTS_DIR/$INSTANCE_ID-outputs.json"
KNOWN_HOSTS_FILE="$ENVIRONMENTS_DIR/$INSTANCE_ID-known_hosts"

EC2_IP=$(jq ".[\"$INSTANCE_ID-BuildEC2\"].PublicIP" "$OUTPUTS_FILE" --raw-output)

echo "Scanning $EC2_IP"

# Wait for deployment, scan SSH to remember EC2 certificate
RETRY_NUM=30
RETRY_EVERY=10
NUM=$RETRY_NUM
until ssh-keyscan -H "$EC2_IP" > "$KNOWN_HOSTS_FILE"
do
  1>&2 echo "Failed SSH scan with status $?, retrying $NUM more times, next in $RETRY_EVERY seconds"
  sleep $RETRY_EVERY
  ((NUM--))

  if [ $NUM -eq 0 ]
  then
    1>&2 echo "SSH scan unsuccessful after $RETRY_NUM tries"
    exit 1
  fi
done
