#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SRC_DIR="${PROJECT_DIR}/src"
CONFIG_FILE="${SCRIPT_DIR}/cloud-config.properties"

FLINK_VERSION="1.13.2"
FLINK_HOME="${HOME}/flink-${FLINK_VERSION}"

echo "=========================================="
echo "Cloud Anomaly Detection Job"
echo "=========================================="
echo "Project Dir: ${PROJECT_DIR}"
echo "Config File: ${CONFIG_FILE}"
echo ""

if [ ! -f "${CONFIG_FILE}" ]; then
    echo "ERROR: Config file not found: ${CONFIG_FILE}"
    exit 1
fi

if [ ! -d "${FLINK_HOME}" ]; then
    echo "WARNING: Flink home not found at ${FLINK_HOME}"
    echo "Running in local mode..."
    RUN_LOCAL=true
fi

cd "${SRC_DIR}"

echo "Building project..."
mvn clean install -DskipTests -q

CLOUD_JAR="${SRC_DIR}/cloud-processing/target/cloud-processing-1.0.0-SNAPSHOT.jar"
COMMON_JAR="${SRC_DIR}/edge-cloud-common/target/edge-cloud-common-1.0.0-SNAPSHOT.jar"
CLOUD_DEPS="${SRC_DIR}/cloud-processing/target/dependency/*"

if [ "${RUN_LOCAL}" = true ]; then
    echo "Running cloud job in local mode..."
    java -cp "${CLOUD_JAR}:${COMMON_JAR}:${CLOUD_DEPS}" \
        com.amazonaws.services.cloud.CloudAnomalyProcessingJob \
        --config "${CONFIG_FILE}" \
        --local true
else
    echo "Submitting job to Flink cluster..."
    "${FLINK_HOME}/bin/flink" run \
        -c com.amazonaws.services.cloud.CloudAnomalyProcessingJob \
        -p 4 \
        "${CLOUD_JAR}" \
        --config "${CONFIG_FILE}"
fi

echo ""
echo "Job submitted successfully!"
echo "Check Flink UI for job status: http://localhost:8081"