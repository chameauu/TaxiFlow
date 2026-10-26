-- Phase 5 run metadata and failure alerts.
-- This file is idempotent because Postgres init scripts only run on a new volume,
-- while the CLI also calls the same statements before every write.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id text PRIMARY KEY,
    kind text NOT NULL,
    status text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    read_rows bigint,
    written_rows bigint,
    quarantined_rows bigint,
    deduplicated_rows bigint,
    checks_total integer,
    checks_failed integer,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    airflow_run_id text
);

CREATE TABLE IF NOT EXISTS ops.pipeline_alerts (
    alert_id bigserial PRIMARY KEY,
    run_id text,
    kind text NOT NULL,
    task_id text,
    severity text NOT NULL DEFAULT 'error',
    message text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pipeline_runs_started_at_idx
    ON ops.pipeline_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS pipeline_alerts_created_at_idx
    ON ops.pipeline_alerts (created_at DESC);
