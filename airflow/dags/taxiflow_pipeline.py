"""TaxiFlow batch, dbt, and quality orchestration DAG."""

from __future__ import annotations

import contextlib
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = os.environ.get("TAXIFLOW_PROJECT_ROOT", "/opt/taxiflow")
VENV = os.environ.get("TAXIFLOW_VENV", "/opt/venv")

DEFAULT_ARGS = {
    "owner": "taxiflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}


def record_failure(context: dict) -> None:
    """Best-effort failure alert that never masks the original Airflow error."""
    task = context.get("task_instance")
    task_id = getattr(task, "task_id", "unknown")
    run_id = context.get("run_id", "unknown")
    message = f"Airflow task {task_id} failed for run {run_id}"
    command = [
        f"{VENV}/bin/taxiflow",
        "runs",
        "failure",
        "--kind",
        "airflow",
        "--message",
        message,
        "--task-id",
        task_id,
        "--airflow-run-id",
        str(run_id),
    ]
    with contextlib.suppress(OSError):
        subprocess.run(command, check=False, capture_output=True, text=True, timeout=30)


with DAG(
    dag_id="taxiflow_pipeline",
    description="Batch TLC -> dbt silver/gold -> quality gate -> promotion marker",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": record_failure},
    tags=["taxiflow", "batch", "dbt", "quality"],
    doc_md="""
# TaxiFlow pipeline

1. `batch` ingests the pinned TLC month into Delta bronze and records counts.
2. `dbt_build` builds silver/gold models and runs dbt tests.
3. `quality` runs GX suites plus cross-layer reconciliation.
4. `promote` is a no-op marker that only runs when every prior task succeeds.

A failed quality gate blocks promotion and writes an alert row to
`ops.pipeline_alerts`; every run records counts in `ops.pipeline_runs`.
""",
) as dag:
    batch = BashOperator(
        task_id="batch",
        bash_command=f"bash {PROJECT_ROOT}/airflow/scripts/run_batch.sh ",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"bash {PROJECT_ROOT}/airflow/scripts/run_dbt.sh ",
    )
    quality = BashOperator(
        task_id="quality",
        bash_command=f"bash {PROJECT_ROOT}/airflow/scripts/run_quality.sh ",
    )
    promote = EmptyOperator(
        task_id="promote",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    batch >> dbt_build >> quality >> promote
