#!/bin/sh
set -eu

: "${SUPERSET_ADMIN_USER:?SUPERSET_ADMIN_USER is required}"
: "${SUPERSET_ADMIN_EMAIL:?SUPERSET_ADMIN_EMAIL is required}"
: "${SUPERSET_ADMIN_PASSWORD:?SUPERSET_ADMIN_PASSWORD is required}"

superset db upgrade
superset init

if ! superset fab list-users | grep -qw "$SUPERSET_ADMIN_USER"; then
    superset fab create-admin \
        --username "$SUPERSET_ADMIN_USER" \
        --firstname TaxiFlow \
        --lastname Admin \
        --email "$SUPERSET_ADMIN_EMAIL" \
        --password "$SUPERSET_ADMIN_PASSWORD"
fi

bundle_root=/opt/taxiflow/config/superset/dashboard
bundle_zip=/tmp/taxiflow-dashboard.zip
python /opt/taxiflow/config/superset/build_bundle.py "$bundle_root" "$bundle_zip"

attempt=1
while [ "$attempt" -le 3 ]; do
    if superset import-dashboards --path "$bundle_zip" --username "$SUPERSET_ADMIN_USER"; then
        break
    fi
    if [ "$attempt" -eq 3 ]; then
        echo "dashboard import failed after 3 attempts" >&2
        exit 1
    fi
    echo "dashboard import attempt $attempt failed; retrying in 10s" >&2
    sleep 10
    attempt=$((attempt + 1))
done
