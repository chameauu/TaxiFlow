#!/usr/bin/env bash
set -euo pipefail

ROOT="${TAXIFLOW_PROJECT_ROOT:-/opt/taxiflow}"
VENV="${TAXIFLOW_VENV:-/opt/venv}"
SPARK_SUBMIT="${SPARK_SUBMIT:-${VENV}/lib/python3.11/site-packages/pyspark/bin/spark-submit}"
SPARK_PACKAGES="${SPARK_PACKAGES:-io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262}"
SERVICE="${TAXIFLOW_BATCH_SERVICE:-yellow}"
YEAR="${TAXIFLOW_BATCH_YEAR:-2024}"
MONTH="${TAXIFLOW_BATCH_MONTH:-1}"

log_file="$(mktemp)"
cleanup() {
    rm -f "${log_file}"
}
trap cleanup EXIT

set +e
"${SPARK_SUBMIT}" \
    --master "local[4]" \
    --driver-memory 4g \
    --conf spark.driver.maxResultSize=1g \
    --conf spark.sql.shuffle.partitions=16 \
    --packages "${SPARK_PACKAGES}" \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    "${ROOT}/src/taxiflow/jobs/batch_bronze.py" \
    --root "${ROOT}" \
    --manifest "${ROOT}/config/tlc_manifest.json" \
    --service "${SERVICE}" \
    --year "${YEAR}" \
    --month "${MONTH}" \
    --write-partitions 8 2>&1 | tee "${log_file}"
status="${PIPESTATUS[0]}"
set -e

if [[ "${status}" -ne 0 ]]; then
    "${VENV}/bin/taxiflow" runs failure \
        --kind batch \
        --message "spark-submit failed with exit code ${status}" \
        --task-id batch || true
    exit "${status}"
fi

metrics="$(grep -E '^TAXIFLOW_METRICS=' "${log_file}" | tail -n 1 || true)"
if [[ -z "${metrics}" ]]; then
    "${VENV}/bin/taxiflow" runs failure \
        --kind batch \
        --message "batch job exited without TAXIFLOW_METRICS" \
        --task-id batch || true
    exit 1
fi

"${VENV}/bin/taxiflow" runs record \
    --kind batch \
    --payload "${metrics#TAXIFLOW_METRICS=}"
