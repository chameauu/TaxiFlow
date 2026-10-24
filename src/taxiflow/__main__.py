"""Command-line entry point for TaxiFlow."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from taxiflow import __version__
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return an exit code."""
    args = build_parser().parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        return 0
    try:
        result: int = handler(args)
    except (ManifestError, ChecksumError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return result


if __name__ == "__main__":
    raise SystemExit(main())
