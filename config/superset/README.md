# Superset serving

Superset reads the batch gold marts from Trino:

```text
trino://taxiflow@trino:8080/delta
```

The dashboard is a batch-serving surface, **not real-time**. The freshness shown on
the dashboard is the batch pipeline freshness. To refresh it:

```bash
make batch
make dbt-build
make quality
make superset-bootstrap
```

`make up` does not start the `bi` profile. Start the full stack with `make up-all`
or start Superset explicitly with the `bi` profile. The audited production path is
the Airflow DAG; `make batch` and `make dbt-build` are developer shortcuts.

## Dashboard contract

The committed YAML bundle under `dashboard/` is imported by `superset-init` with
`superset import-dashboards --path ... --username ...`. The importer uses
`overwrite=True`, so rerunning the bootstrap updates the dashboard instead of
creating duplicates.

The SQL definitions under `queries/` document the same metrics used by the
dashboard. They must not use `SELECT *`, and must not sum per-trip measures such as
`passenger_count` or `trip_distance`.

## Refresh expectations

- Batch TLC data: refreshed by the daily Airflow DAG, or manually with `make batch`
  followed by `make dbt-build`.
- Quality/freshness panel: refreshed by `make quality`; `batch_data_age_seconds` is the
  age of the newest batch ingestion, while `cdc_lag_seconds` is the event-time age of
  the newest CDC trip in gold.
- CDC trips are part of the dashboard through `fct_trips.record_source`.
