"""Command-line entry point for TaxiFlow."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

from taxiflow import __version__
from taxiflow.quality.metadata import (
    MetadataError,
    connect_postgres,
    ensure_schema,
    list_runs,
    parse_metrics_line,
    record_failure,
    record_payload,
)
from taxiflow.quality.runner import QualityConfig, QualityError, print_report, run_quality
from taxiflow.quality.suites import SuiteError
from taxiflow.streaming.contract import (
    COMPATIBILITY,
    CONNECTOR_CONFIG_PATH,
    VALUE_SUBJECT,
    CdcError,
    load_connector_config,
)
from taxiflow.streaming.registry import register_cdc, wait_for_latest_schema
from taxiflow.tlc.manifest import (
    SERVICES,
    ChecksumError,
    ManifestError,
    TlcFile,
    download_file,
    load_manifest,
    select_file,
    verify_file,
)


def _add_tlc_commands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    tlc = subparsers.add_parser("tlc", help="download and verify TLC source files")
    tlc_subparsers = tlc.add_subparsers(dest="tlc_command", required=True)

    for name, help_text in (
        ("download", "download missing files and verify checksums"),
        ("verify", "verify cached files against the manifest"),
    ):
        command = tlc_subparsers.add_parser(name, help=help_text)
        command.add_argument(
            "--manifest",
            default="config/tlc_manifest.json",
            help="path to the TLC manifest",
        )
        command.add_argument(
            "--root",
            default=".",
            help="project root used to resolve manifest paths",
        )
        command.add_argument("--service", choices=sorted(SERVICES))
        command.add_argument("--year", type=int)
        command.add_argument("--month", type=int)
        command.set_defaults(handler=_download if name == "download" else _verify)


def _selected_files(args: argparse.Namespace) -> tuple[TlcFile, ...]:
    root = Path(args.root).resolve()
    files = load_manifest(root / args.manifest)
    if args.service is None:
        return files
    if args.year is None or args.month is None:
        raise ManifestError("--service requires --year and --month")
    return (select_file(files, args.service, args.year, args.month),)


def _download(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = _selected_files(args)
    for entry in files:
        destination = root / entry.path
        existed = destination.is_file()
        path = download_file(entry, root)
        action = "verified" if existed else "downloaded"
        print(f"{action} {path} ({entry.size_bytes} bytes, sha256 {entry.sha256})")
    return 0


def _verify(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    failures = 0
    for entry in _selected_files(args):
        try:
            path = verify_file(entry, root)
        except (ChecksumError, FileNotFoundError) as exc:
            failures += 1
            print(f"FAIL {entry.path}: {exc}", file=sys.stderr)
        else:
            print(f"ok {path} ({entry.size_bytes} bytes, sha256 {entry.sha256})")
    return 1 if failures else 0


def _add_cdc_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    cdc = subparsers.add_parser("cdc", help="register and inspect the CDC contract")
    cdc_subparsers = cdc.add_subparsers(dest="cdc_command", required=True)

    register = cdc_subparsers.add_parser(
        "register",
        help="register the Postgres connector and enforce Avro compatibility",
    )
    register.add_argument(
        "--connector-config",
        type=Path,
        default=CONNECTOR_CONFIG_PATH,
        help="connector payload with environment placeholders",
    )
    register.add_argument(
        "--debezium-url",
        default="http://localhost:8083",
        help="Kafka Connect REST base URL",
    )
    register.add_argument(
        "--schema-registry-url",
        default="http://localhost:8081",
        help="Schema Registry base URL",
    )
    register.add_argument("--compatibility", default=COMPATIBILITY)
    register.add_argument("--timeout", type=float, default=120.0)
    register.set_defaults(handler=_register_cdc)

    schema = cdc_subparsers.add_parser("schema", help="print the latest Avro schema")
    schema.add_argument(
        "--schema-registry-url",
        default="http://localhost:8081",
        help="Schema Registry base URL",
    )
    schema.add_argument("--subject", default=VALUE_SUBJECT)
    schema.add_argument("--timeout", type=float, default=120.0)
    schema.set_defaults(handler=_print_schema)


def _register_cdc(args: argparse.Namespace) -> int:
    payload = load_connector_config(args.connector_config)
    result = register_cdc(
        args.debezium_url,
        args.schema_registry_url,
        payload,
        compatibility=args.compatibility,
        timeout=args.timeout,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _print_schema(args: argparse.Namespace) -> int:
    schema = wait_for_latest_schema(
        args.schema_registry_url,
        args.subject,
        timeout=args.timeout,
    )
    print(json.dumps(schema, indent=2, sort_keys=True))
    return 0


def _add_quality_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    quality = subparsers.add_parser("quality", help="run bronze/silver quality gates")
    quality_subparsers = quality.add_subparsers(dest="quality_command", required=True)
    run = quality_subparsers.add_parser(
        "run",
        help="run GX suites and cross-layer reconciliation",
    )
    run.add_argument(
        "--max-freshness-seconds",
        type=int,
        help="override TAXIFLOW_QUALITY_MAX_FRESHNESS_SECONDS",
    )
    run.add_argument(
        "--sample-limit",
        type=int,
        help="override TAXIFLOW_QUALITY_SAMPLE_LIMIT",
    )
    run.add_argument(
        "--no-record",
        action="store_true",
        help="skip Postgres run metadata",
    )
    run.set_defaults(handler=_quality_run)

    runs = subparsers.add_parser("runs", help="inspect and record pipeline runs")
    run_subparsers = runs.add_subparsers(dest="runs_command", required=True)

    record = run_subparsers.add_parser("record", help="record one successful run")
    record.add_argument("--kind", required=True, choices=("batch", "dbt", "quality", "airflow"))
    record.add_argument("--payload", help="JSON payload, for example TAXIFLOW_METRICS")
    record.add_argument("--payload-file", type=Path)
    record.add_argument("--batch-log", type=Path, help="log containing TAXIFLOW_METRICS=")
    record.add_argument("--status", default="success", choices=("success", "failed"))
    record.add_argument("--airflow-run-id")
    record.set_defaults(handler=_runs_record)

    failure = run_subparsers.add_parser("failure", help="record a failed run and alert")
    failure.add_argument("--kind", required=True, choices=("batch", "dbt", "quality", "airflow"))
    failure.add_argument("--message", required=True)
    failure.add_argument("--task-id")
    failure.add_argument("--run-id")
    failure.add_argument("--airflow-run-id")
    failure.set_defaults(handler=_runs_failure)

    ensure = run_subparsers.add_parser("ensure-schema", help="create ops tables")
    ensure.set_defaults(handler=_runs_ensure)

    show = run_subparsers.add_parser("show", help="print recent runs")
    show.add_argument("--limit", type=int, default=10)
    show.set_defaults(handler=_runs_show)


def _quality_run(args: argparse.Namespace) -> int:
    config = QualityConfig.from_env()
    if args.max_freshness_seconds is not None:
        config = replace(config, max_freshness_seconds=args.max_freshness_seconds)
    if args.sample_limit is not None:
        config = replace(config, sample_limit=args.sample_limit)
    report = run_quality(config, record=not args.no_record)
    print_report(report)
    return 0 if report.success else 1


def _load_payload(args: argparse.Namespace) -> dict[str, object]:
    if args.batch_log is not None:
        return parse_metrics_line(args.batch_log.read_text())
    if args.payload_file is not None:
        payload = json.loads(args.payload_file.read_text())
    elif args.payload is not None:
        payload = json.loads(args.payload)
    else:
        payload = {}
    if not isinstance(payload, dict):
        raise MetadataError("run payload must be a JSON object")
    return payload


def _runs_record(args: argparse.Namespace) -> int:
    record = record_payload(
        kind=args.kind,
        payload=_load_payload(args),
        status=args.status,
        airflow_run_id=args.airflow_run_id,
    )
    print(record.run_id)
    return 0


def _runs_failure(args: argparse.Namespace) -> int:
    record_failure(
        kind=args.kind,
        message=args.message,
        task_id=args.task_id,
        run_id=args.run_id,
        airflow_run_id=args.airflow_run_id,
    )
    return 0


def _runs_ensure(args: argparse.Namespace) -> int:
    connection = connect_postgres()
    try:
        ensure_schema(connection)
    finally:
        connection.close()
    print("ops schema ready")
    return 0


def _runs_show(args: argparse.Namespace) -> int:
    print(json.dumps(list_runs(args.limit), indent=2, sort_keys=True, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI parser."""
    parser = argparse.ArgumentParser(
        prog="taxiflow",
        description="An NYC TLC batch + CDC lakehouse.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    subparsers = parser.add_subparsers(dest="command")
    _add_tlc_commands(subparsers)
    _add_cdc_commands(subparsers)
    _add_quality_commands(subparsers)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return an exit code."""
    args = build_parser().parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        return 0
    try:
        result: int = handler(args)
    except (
        ManifestError,
        ChecksumError,
        CdcError,
        MetadataError,
        QualityError,
        SuiteError,
        FileNotFoundError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return result


if __name__ == "__main__":
    raise SystemExit(main())
