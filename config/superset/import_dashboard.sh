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
superset import-dashboards --path "$bundle_zip" --username "$SUPERSET_ADMIN_USER"
