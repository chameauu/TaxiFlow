#!/usr/bin/env bash
set -euo pipefail

ROOT="${TAXIFLOW_PROJECT_ROOT:-/opt/taxiflow}"
VENV="${TAXIFLOW_VENV:-/opt/venv}"

cd /tmp
set +e
"${VENV}/bin/dbt" build \
    --project-dir "${ROOT}/dbt" \
    --profiles-dir "${ROOT}/dbt" \
    --target-path /tmp/taxiflow-dbt-target \
    --log-path /tmp/taxiflow-dbt-logs
status=$?
set -e

if [[ "${status}" -ne 0 ]]; then
    "${VENV}/bin/taxiflow" runs failure \
        --kind dbt \
        --message "dbt build failed with exit code ${status}" \
        --task-id dbt_build || true
    exit "${status}"
fi

"${VENV}/bin/taxiflow" runs record \
    --kind dbt \
    --payload '{"command":"dbt build","project":"dbt","target":"dev"}'
