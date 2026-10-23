# TaxiFlow

An NYC TLC batch + CDC lakehouse.

**Status:** Phase 0 scaffold complete locally; Phase 1 not started.

The target architecture, phases, and exit gates are in `../PLAN.md`.
Current status and decisions are in `../PROGRESS.md`.
A learning-oriented explanation of the work is in `../understanding/`.

## Layout

```text
src/taxiflow/   Python package
tests/          unit, integration, and contract tests
dbt/            dbt models and profiles
airflow/        Airflow DAGs
config/         runtime configuration
compose.yaml    one local Compose project
```

## Verify

```bash
uv sync --frozen
uv run ruff check .
uv run pytest -q
docker compose config -q
```
