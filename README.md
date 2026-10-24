# TaxiFlow

An NYC TLC batch + CDC lakehouse.

**Status:** Phases 0–4 complete locally. Phase 5 (orchestration and quality) is next.

The target architecture, phases, and exit gates are in `../PLAN.md`.
Current status and decisions are in `../PROGRESS.md`.
A learning-oriented explanation is in `../understanding/`.

## What works now

- One Compose project with `core`, `stream`, `orchestration`, `bi`, and `batch` profiles.
- Healthchecked MinIO, PostgreSQL, Hive Metastore, Trino, Kafka, Schema Registry,
  Debezium, Airflow, and Superset.
- A checksum-pinned TLC manifest and `taxiflow tlc download|verify` CLI.
- A Spark batch job that writes replay-safe Delta bronze and quarantine tables in MinIO.
- dbt staging, intermediate, and gold models on Trino, with 66 data tests and
  reconciliation checks over 2,964,606 trips.
- An Avro CDC path from Postgres through Debezium, Kafka, and Spark Structured Streaming
  into partitioned Delta bronze, with delete tombstones and checkpoint recovery.
- Trino readback of bronze and gold tables through the Delta connector.

```text
TLC monthly Parquet ── batch_bronze ──► Delta bronze (MinIO)
                               └──────► quarantine (MinIO)
                                               │
                                               ▼
                                  dbt on Trino (silver → gold)
                                               │
                                               ▼
                                      Trino (Delta connector)

Postgres iot.* ── Debezium ──► Kafka ──► Spark Structured Streaming
                                                    │
                                                    ▼
                                      Delta CDC bronze (MinIO)
```

## Prerequisites

- Docker with Compose v2.
- Python 3.11 and `uv`.
- About 8 GB of free memory for the full local stack; Spark uses a 4 GiB driver heap.

## Quick start

```bash
make env
uv sync --frozen
make up
make seed
make batch
make dbt-build
make batch-report
make cdc-report
```

`make seed` downloads and verifies the pinned 2024-01 yellow TLC file, applies the CDC
source schema, registers the Avro connector, and starts the synthetic generator plus
Spark stream. `make batch` writes Delta bronze and quarantine tables. `make dbt-build`
loads the taxi-zone seed, builds silver/gold models, and runs the dbt tests.
`make batch-report` reads batch bronze back; `make cdc-report` reads CDC bronze back and
enforces the 60-second freshness gate.

## Verify

```bash
uv run ruff check .
make verify
make cdc-replay
uv run pytest -q -m integration
uv run dbt build --project-dir dbt --profiles-dir dbt
```

`make verify` runs lint, unit tests, contract tests, and Compose validation.
`make cdc-replay` replays a deterministic CDC batch and proves no duplicate keys, the
newer update wins, and deletes become tombstones. The integration suite needs the local
stack and Spark. The dbt build needs Trino plus the registered Delta bronze tables;
`make dbt-build` registers them first.

## Layout

```text
src/taxiflow/       Python package, CLI, TLC contracts, Spark jobs
tests/              unit, contract, integration, and tiny Parquet fixtures
dbt/                dbt project: seeds, staging, intermediate, marts, tests
airflow/            Airflow DAGs (Phase 5)
config/             TLC manifest, Avro contract, Debezium, Postgres, and service config
docker/debezium/    Debezium image with the Confluent Avro converter
compose.yaml        one local Compose project
```

## Data and secrets

- `data/` is gitignored; run `make seed` after a fresh checkout.
- `.env` is local and gitignored; `.env.example` documents required keys.
- No credentials or downloaded TLC data belong in git.
