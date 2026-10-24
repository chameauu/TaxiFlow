# TaxiFlow

An NYC TLC batch + CDC lakehouse.

**Status:** Phases 0–2 complete locally. Phase 3 (dbt silver/gold) is next.

The target architecture, phases, and exit gates are in `../PLAN.md`.
Current status and decisions are in `../PROGRESS.md`.
A learning-oriented explanation is in `../understanding/`.

## What works now

- One Compose project with `core`, `stream`, `orchestration`, `bi`, and `batch` profiles.
- Healthchecked MinIO, PostgreSQL, Hive Metastore, Trino, Kafka, Schema Registry,
  Debezium, Airflow, and Superset.
- A checksum-pinned TLC manifest and `taxiflow tlc download|verify` CLI.
- A Spark batch job that writes replay-safe Delta bronze and quarantine tables in MinIO.
- Trino readback of the bronze table through the Delta connector.

```text
TLC monthly Parquet ── batch_bronze ──► Delta bronze (MinIO)
                               └──────► quarantine (MinIO)
                                               │
                                               ▼
                                      Trino (Delta connector)
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
make batch-report
```

`make seed` downloads and verifies the pinned 2024-01 yellow TLC file. `make batch`
writes Delta bronze and quarantine tables. `make batch-report` reads them back.

## Verify

```bash
uv run ruff check .
uv run pytest -q
uv run pytest -q -m contract
uv run pytest -q -m integration
docker compose config -q
```

The integration suite needs the local stack and Spark. It includes a two-run replay
test for the batch job.

## Layout

```text
src/taxiflow/       Python package, CLI, TLC contracts, Spark jobs
tests/              unit, contract, integration, and tiny Parquet fixtures
dbt/                dbt project (Phase 3)
airflow/            Airflow DAGs (Phase 5)
config/             TLC manifest and service configuration
compose.yaml        one local Compose project
```

## Data and secrets

- `data/` is gitignored; run `make seed` after a fresh checkout.
- `.env` is local and gitignored; `.env.example` documents required keys.
- No credentials or downloaded TLC data belong in git.
