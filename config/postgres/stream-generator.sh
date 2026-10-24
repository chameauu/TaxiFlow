#!/usr/bin/env bash
set -euo pipefail

interval="${TAXIFLOW_EVENT_INTERVAL:-5}"
echo "synthetic CDC generator started; interval=${interval}s"

while true; do
  psql -v ON_ERROR_STOP=1 -q -c "SELECT iot.emit_synthetic_trip();" >/dev/null
  sleep "$interval"
done
