#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SRC_DIR="${PROJECT_DIR}/src"
CONFIG_FILE="${SCRIPT_DIR}/edge-config.properties"

FLINK_VERSION="1.13.2"
FLINK_HOME="${HOME}/flink-${FLINK_VERSION}"

echo "=========================================="
echo "Edge Anomaly Detection Job"
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

EDGE_JAR="${SRC_DIR}/edge-processing/target/edge-processing-1.0.0-SNAPSHOT.jar"
COMMON_JAR="${SRC_DIR}/edge-cloud-common/target/edge-cloud-common-1.0.0-SNAPSHOT.jar"
EDGE_DEPS="${SRC_DIR}/edge-processing/target/dependency/*"

if [ "${RUN_LOCAL}" = true ]; then
    echo "Running edge job in local mode..."
    java -cp "${EDGE_JAR}:${COMMON_JAR}:${EDGE_DEPS}" \
        com.amazonaws.services.edge.EdgeAnomalyProcessingJob \
        --config "${CONFIG_FILE}" \
        --local true
else
    echo "Submitting job to Flink cluster..."
    "${FLINK_HOME}/bin/flink" run \
        -c com.amazonaws.services.edge.EdgeAnomalyProcessingJob \
        "${EDGE_JAR}" \
        --config "${CONFIG_FILE}"
fi

echo ""
echo "Job submitted successfully!"
echo "Check Flink UI for job status: http://localhost:8081"