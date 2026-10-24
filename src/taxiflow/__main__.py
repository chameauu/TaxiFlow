"""Command-line entry point for TaxiFlow."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from taxiflow import __version__
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return an exit code."""
    args = build_parser().parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        return 0
    try:
        result: int = handler(args)
    except (ManifestError, ChecksumError, CdcError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return result


if __name__ == "__main__":
    raise SystemExit(main())
