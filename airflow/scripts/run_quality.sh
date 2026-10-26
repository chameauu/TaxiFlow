#!/usr/bin/env bash
set -euo pipefail

VENV="${TAXIFLOW_VENV:-/opt/venv}"
MAX_FRESHNESS="${TAXIFLOW_QUALITY_MAX_FRESHNESS_SECONDS:-86400}"

exec "${VENV}/bin/taxiflow" quality run \
    --max-freshness-seconds "${MAX_FRESHNESS}"
