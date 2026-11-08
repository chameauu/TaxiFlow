# TaxiFlow

**An NYC TLC batch + CDC lakehouse.** One Compose project, one Delta lake on MinIO, dbt
models on Trino, and a Superset dashboard.

A fresh checkout reaches queryable gold tables with
`make up && make seed && make batch && make dbt-build`.

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Stack](#stack)
- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Command reference](#command-reference)
- [Data model](#data-model)
- [Orchestration](#orchestration)
- [CDC contract](#cdc-contract)
- [Serving](#serving)
- [Verification](#verification)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## Overview

TaxiFlow ingests NYC TLC trip data in two ways and lands both in one governed lake:

1. **Batch** — monthly TLC Parquet files, checksum-pinned in `config/tlc_manifest.json`,
   normalized against explicit per-service schemas, written to Delta bronze with
   deterministic trip identity and a queryable quarantine.
2. **CDC** — synthetic OLTP trips in Postgres (`iot.taxi_nyc_time_series`), streamed
   through Debezium and Kafka with an Avro contract enforced by Schema Registry, then
   written to partitioned Delta bronze by Spark Structured Streaming.

dbt runs on Trino over the Delta bronze tables, producing conformed silver models and gold
marts. Superset reads those marts through the Trino Delta connector.

Design rules the code follows:

- **One lake, one layout.** Bronze, silver, and gold are Delta tables in MinIO. There is no
  second Parquet warehouse and no dead-end Delta branch.
- **Replay-safe by construction.** Bronze writes are deterministic and partition-scoped;
  silver/gold are rebuilt, not appended. Re-running a batch does not change counts or sums.
- **Explicit schemas.** No `inferSchema` in committed jobs; schema drift is absorbed by a
  per-service column contract and recorded in quarantine.
- **Deterministic identity.** TLC has no natural trip key, so `trip_id` is derived from
  source provenance plus row content; CDC events carry a stable `trip_id` from the source.
- **Measured quality.** Every run reports read, written, quarantined, and deduplicated
  rows; freshness is a queryable column, not an assumption.

## Architecture

```text
TLC monthly Parquet ── batch_bronze ──► Delta bronze  (MinIO, partitioned)
                              └──────► Delta quarantine (MinIO)
                                            │
                                            ▼
                              dbt on Trino: staging → intermediate → marts
                                            │
                                            ▼
                                 gold marts (Delta) ──► Trino ──► Superset

Postgres iot.taxi_nyc_time_series ── Debezium (Avro) ──► Kafka ──► Spark Streaming
                                                              │
                                                              ▼
                                                  Delta CDC bronze (MinIO)
```

Batch and CDC publish to the same lake. Gold marts are currently derived from batch bronze
only; CDC is not yet unified in the dbt models.

## Stack

| Component | Image / version | Role |
|---|---|---|
| Python | 3.11 (`uv`, committed `uv.lock`) | Package, CLI, jobs, tests |
| MinIO | `bitnamilegacy/minio:2025.7.23-debian-12-r5` | S3-compatible object store for the lake |
| PostgreSQL | `postgres:15.19` | OLTP source for CDC, Airflow metadata, Superset metadata |
| Hive Metastore | `starburstdata/hive:3.1.2-e.18` | Table metadata for Trino |
| Trino | `trinodb/trino:476` | SQL engine over Delta; dbt targets Trino |
| Spark | `apache/spark:3.5.1` + Delta 3.1.0 | Batch ingestion and Structured Streaming |
| Kafka / Schema Registry | Confluent `7.5.0` | CDC transport and Avro compatibility |
| Debezium | `taxiflow/debezium:2.5.1-avro` | Postgres logical decoding with Confluent Avro |
| Airflow | `taxiflow/airflow:2.9.3` (base `apache/airflow:2.9.3-python3.11`) | Batch, dbt, and quality orchestration |
| Superset | `taxiflow/superset:4.1.1` | BI over Trino gold marts |
| dbt | `dbt-trino` (`pyproject.toml`) | Silver and gold transformations |
| Great Expectations | `great-expectations==0.18.22` | Bronze and silver data-quality suites |

Every image is pinned; the three `taxiflow/*` images are built locally from `docker/`.

## Repository layout

| Path | Contents |
|---|---|
| `src/taxiflow/` | Package: TLC manifest/schema/transform, batch and streaming jobs, quality runner, serving helpers, CLI |
| `tests/` | `unit/`, `contract/`, `integration/`, and tiny committed Parquet fixtures |
| `dbt/` | dbt project: seeds, staging, intermediate, marts, schema tests, reconciliation singletons |
| `airflow/` | DAG and the batch/dbt/quality wrapper scripts |
| `config/` | TLC manifest, Avro contract, Debezium connector, Postgres init, Trino config, Superset assets |
| `docker/` | Local images for Airflow, Debezium, and Superset |
| `docker-compose.yaml` | The single Compose project with `core`, `stream`, `orchestration`, `bi`, `batch` profiles |
| `Makefile` | The supported interface: `make help` lists every target |

## Prerequisites

- Docker Engine with Compose v2.
- Python 3.11 and [`uv`](https://docs.astral.sh/uv/).
- ~8 GB free memory (Spark runs a 4 GiB driver heap) and ~10 GB free disk for images,
  the pinned TLC slice, and Delta data.
- These host ports must be free: `8080` (Airflow), `8081` (Schema Registry), `8083`
  (Debezium), `8084` (Trino), `8088` (Superset), `9000`/`9001` (MinIO), `9092` (Kafka),
  `9083` (Hive Metastore), `5432` (Postgres).

## Quick start

```bash
make env                 # writes .env with generated local credentials
uv sync --frozen --group quality
make up                  # core + stream profiles, all healthchecked
make seed                # download/verify TLC file, apply CDC schema, register Debezium
make batch               # 2024-01 yellow → Delta bronze + quarantine
make dbt-build           # seeds, silver, gold, and dbt tests
make batch-report        # bronze counts and sums
```

Add the analytical and orchestration layers:

```bash
make up-all              # adds orchestration (Airflow) and bi (Superset)
make superset-bootstrap  # create the admin, import the dashboard bundle
make serving-verify      # Trino connection, datasets, dashboard, committed SQL
```

`make up` starts `core` and `stream` only. Anything that needs Airflow or Superset —
including the dashboard — requires `make up-all` or an explicit `--profile`.

## Command reference

| Command | Purpose |
|---|---|
| `make env` | Create `.env` with generated credentials (never committed) |
| `make up` / `make up-all` | Start `core`+`stream` / also `orchestration`+`bi`, waiting for health |
| `make down` / `make reset` | Stop services / stop and delete named volumes |
| `make ps` / `make logs` | Service health / aggregated logs |
| `make images` / `make pull` | List / pull every image, including the `batch` profile |
| `make seed` | Download and verify TLC data; initialize and register CDC |
| `make batch` / `make batch-report` | Run one TLC month into bronze / read it back |
| `make cdc-init` / `cdc-seed` / `cdc-register` / `cdc-start` / `cdc-stop` | CDC lifecycle |
| `make cdc-report` / `make cdc-replay` | CDC counts and freshness / deterministic replay drill |
| `make register-sources` | Register Delta bronze tables with Trino |
| `make dbt-debug` / `make dbt-build` | Check the Trino connection / build seeds, models, tests |
| `make quality` | GX suites plus bronze→silver→gold reconciliation |
| `make superset-bootstrap` / `make serving-verify` | Import the dashboard / verify the serving path |
| `make test` / `test-contract` / `test-integration` | Unit / contract / stack tests |
| `make lint` / `make format` / `make compose-check` | Ruff / Ruff / Compose validation |
| `make verify` | Lint, unit tests, contract tests, Compose validation |

## Data model

**Bronze** (MinIO, Delta, partitioned by `service`, `pickup_year`, `pickup_month`):

- `tlc_trips` — accepted rows with source values preserved, plus `trip_id`, `content_hash`,
  `source_file`, and `ingested_at`.
- `tlc_quarantine` — rejected rows with `quarantine_reason` and the raw payload.
- CDC bronze is partitioned by `event_date` and keeps delete tombstones.

**Silver** (dbt on Trino): `stg_yellow_trips`, `stg_taxi_zones`, `stg_tlc_quarantine`, and
`int_trips_enriched` (typed, deduplicated, zone-enriched). Bronze keeps the naive source
wall-clock values; staging interprets them as `America/New_York` and produces
`timestamp with time zone`.

**Gold** (dbt marts):

| Model | Grain |
|---|---|
| `fct_trips` | One row per accepted trip |
| `dim_zones` | One row per `LocationID` from the official lookup |
| `agg_daily_trips` | Date × service |
| `agg_zone_daily` | Date × zone × vendor × service |
| `agg_hourly_demand` | Weekday × hour × zone × service |
| `agg_tip_rate` | Payment type × borough |
| `quality_summary` | One row of counts and freshness |
| `fct_quality_reasons` | Quarantine reason counts |

Rules: money is cast to `DECIMAL(18,2)` in the gold marts; `passenger_count` and
`trip_distance` are per-trip measures and are never summed across grouped trips; unmapped
zones (`264`, `265`) are kept explicitly rather than dropped.

## Orchestration

The Airflow DAG `taxiflow_pipeline` runs `batch → dbt_build → quality → promote`. A failed
quality check blocks `promote` and writes a row to `ops.pipeline_alerts`; every run records
counts in `ops.pipeline_runs`. The audited path is the DAG — `make batch` and
`make dbt-build` are developer shortcuts that run the same code.

```bash
docker compose --profile core --profile orchestration exec -T airflow \
  airflow dags test taxiflow_pipeline 2026-11-08
```

Streaming is a long-lived service, not a DAG task. Spark Structured Streaming checkpoints
live under the CDC table prefix, so a restart resumes from the last committed offset.

## CDC contract

- Logical decoding reads the `iot.taxi_nyc_time_series` table; Debezium emits `before`/`after` with
  `op`, `ts_ms`, and a stable `trip_id`.
- The event schema is Avro (`config/avro/taxi_trip_event.avsc`) and Schema Registry
  enforces `BACKWARD_TRANSITIVE` compatibility. JSON is acceptable only for local
  debugging, never as the committed contract.
- Deletes arrive as tombstones; the stream job applies them so downstream state matches
  the source.
- A malformed record fails the job loudly rather than being dropped silently.

## Serving

Superset reads batch gold marts through Trino at `trino://taxiflow@trino:8080/delta`.
The dashboard (`TaxiFlow NYC TLC Overview`) answers four questions — daily trips and
revenue, zone/vendor performance, demand by hour and weekday, tip rate by payment type
and borough — plus a data-quality/freshness panel.

Assets are versioned, not generated at runtime:

- `config/superset/dashboard/` — importable YAML bundle (metadata, database, datasets,
  charts, dashboard).
- `config/superset/queries/` — documented SQL for every metric; checked for `SELECT *`
  and for summing per-trip measures.
- `config/superset/import_dashboard.sh` — `superset db upgrade`, `superset init`,
  idempotent admin creation, deterministic ZIP build, `superset import-dashboards`.

The dashboard is a **batch** surface. `freshness_seconds` is the age of the newest batch
ingestion, not streaming latency. To refresh: `make batch` → `make dbt-build` →
`make quality` → `make superset-bootstrap`. Re-running the bootstrap updates the dashboard
in place instead of creating duplicates.

## Verification

| Layer | Location | Requires |
|---|---|---|
| Unit | `tests/unit/` | Nothing — deterministic, no Docker or network |
| Contract | `tests/contract/` | Nothing — asserts Compose, config, and asset invariants |
| Integration | `tests/integration/` | The running stack (`-m integration`, `TAXIFLOW_REQUIRE_STACK=1`) |
| dbt data tests | `dbt/models/schema.yml`, `dbt/tests/` | Trino plus registered bronze tables |
| Quality | GX suites + `taxiflow quality` | MinIO, Postgres, Trino |

```bash
make verify            # ruff + 55 unit + 54 contract + Compose validation
make quality           # GX suites and full-table reconciliation
make serving-verify    # Superset ↔ Trino ↔ gold, including every committed query
make cdc-replay        # replay a CDC batch twice; counts and keys must not change
uv run pytest -q -m integration
```

`make verify` is the pre-commit gate; `make quality`, `make cdc-replay`, and
`make serving-verify` need the running stack. Every gate is expected to pass from a clean
volume, not only on an existing one.

## Configuration

Configuration is environment-only; `.env.example` documents every key and `.env` is local
and gitignored.

| Group | Keys |
|---|---|
| Storage | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `S3_BUCKET` |
| Postgres | `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` |
| Airflow | `AIRFLOW_FERNET_KEY`, `AIRFLOW_WEBSERVER_SECRET_KEY`, `AIRFLOW_ADMIN_*` |
| Superset | `SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_*`, `SUPERSET_TRINO_URI` |
| Trino | `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG`, `TRINO_BRONZE_SCHEMA` |
| dbt | `DBT_SCHEMA`, `DBT_SILVER_SCHEMA`, `DBT_GOLD_SCHEMA` |
| Batch/quality | `TAXIFLOW_BATCH_*`, `TAXIFLOW_EVENT_INTERVAL`, `TAXIFLOW_QUALITY_*` |

`make env` generates random credentials. Nothing in `docker-compose.yaml`, the dbt
profile, or the Superset bundle contains a secret; the tracked baseline credentials from
the upstream reference are never reused. `data/` and `.env` are gitignored.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `make up` succeeds but Superset is missing | `bi` is not in the default profiles | `make up-all` or `--profile bi` |
| Dashboard opens but panels error | Trino/HMS not ready or tables unregistered | `make register-sources`, then `make serving-verify` |
| `superset import-dashboards` raises `JSONDecodeError` | Bundle files sit at the ZIP root; v1 importer strips one path component | Keep the root folder (the builder always writes `taxiflow-dashboard/`) |
| ZIP build fails with "timestamps before 1980" | Exported YAML mtimes | The builder writes fixed `ZipInfo` timestamps |
| Superset API returns "CSRF token is missing" | Mutating calls need a session cookie | Log in, `GET /api/v1/security/csrf_token/`, send `X-CSRFToken` |
| Dashboard revenue looks stale | `make batch` writes bronze only | Run `make dbt-build` (and `make quality`) before refreshing |
| Postgres is reachable but the DAG cannot connect | `POSTGRES_HOST` is host-scoped | Use the Compose service name `postgres` |

## Provenance

This project was developed against a public upstream reference pipeline that is kept
read-only and is not part of this repository. It has not been publicly released; any future
publication will state whether it is a fork with attribution or an original implementation
with a fresh history.
