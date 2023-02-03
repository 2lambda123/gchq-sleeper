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

if [ "$#" -ne 2 ]; then
	echo "Usage: $0 <uniqueId> <table-name>"
	exit 1
fi

INSTANCE_ID=$1
TABLE_NAME=$2

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd .. && pwd)

# Download latest instance configuration
"${THIS_DIR}/downloadConfig.sh" "${INSTANCE_ID}" || true

TEMPLATE_DIR=${SCRIPTS_DIR}/templates
GENERATED_DIR=${SCRIPTS_DIR}/generated
JAR_DIR=${SCRIPTS_DIR}/jars
INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
TABLE_DIR=${GENERATED_DIR}/tables/${TABLE_NAME}
TABLE_PROPERTIES=${TABLE_DIR}/table.properties
SCHEMA=${TABLE_DIR}/schema.json

echo "-------------------------------------------------------------------------------"
echo "Running Deployment"
echo "-------------------------------------------------------------------------------"
echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "TABLE_NAME: ${TABLE_NAME}"
echo "TEMPLATE_DIR: ${TEMPLATE_DIR}"
echo "GENERATED_DIR:${GENERATED_DIR}"
echo "INSTANCE_PROPERTIES: ${INSTANCE_PROPERTIES}"
echo "TABLE_PROPERTIES: ${TABLE_PROPERTIES}"
echo "SCHEMA: ${SCHEMA}"
echo "SCRIPTS_DIR: ${SCRIPTS_DIR}"
echo "JAR_DIR: ${JAR_DIR}"

VERSION=$(cat "${TEMPLATE_DIR}/version.txt")
echo "VERSION: ${VERSION}"

echo "Generating properties"

mkdir -p "${TABLE_DIR}"

# Schema
cp "${TEMPLATE_DIR}/schema.template" "${SCHEMA}"

# Table Properties
sed \
  -e "s|^sleeper.table.name=.*|sleeper.table.name=${TABLE_NAME}|" \
	"${TEMPLATE_DIR}/tableproperties.template" \
	> "${TABLE_PROPERTIES}"

echo "-------------------------------------------------------"
echo "Deploying Stacks"
echo "-------------------------------------------------------"
cdk -a "java -cp ${JAR_DIR}/cdk-${VERSION}.jar sleeper.cdk.SleeperCdkApp" deploy \
--require-approval never -c propertiesfile="${INSTANCE_PROPERTIES}" -c validate=false "*"
