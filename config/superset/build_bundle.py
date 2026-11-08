"""Build the Superset dashboard ZIP from the committed YAML bundle."""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

# Superset exports often carry a 1980-01-01 mtime; using a fixed timestamp
# also makes the generated ZIP byte-for-byte reproducible.
FIXED_ZIP_DATE_TIME = (1980, 1, 1, 0, 0, 0)
BUNDLE_ROOT = "taxiflow-dashboard"


def build_bundle(source: Path, destination: Path) -> Path:
    """Write a deterministic ZIP containing every file under ``source``."""
    if not source.is_dir():
        raise FileNotFoundError(f"dashboard bundle directory not found: {source}")

    files = sorted(path for path in source.rglob("*") if path.is_file())
    if not files:
        raise ValueError(f"dashboard bundle is empty: {source}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            info = zipfile.ZipInfo(
                f"{BUNDLE_ROOT}/{path.relative_to(source).as_posix()}",
                date_time=FIXED_ZIP_DATE_TIME,
            )
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, path.read_bytes())
    return destination


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        raise SystemExit("usage: build_bundle.py SOURCE_DIRECTORY DESTINATION_ZIP")
    build_bundle(Path(argv[1]), Path(argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
