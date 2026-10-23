"""Command-line entry point for TaxiFlow."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from taxiflow import __version__


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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return an exit code."""
    build_parser().parse_args(argv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
