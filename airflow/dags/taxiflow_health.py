from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="taxiflow_health",
    description="Minimal DAG proving the Airflow control plane is alive.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["taxiflow", "health"],
) as dag:
    EmptyOperator(task_id="health_check")
