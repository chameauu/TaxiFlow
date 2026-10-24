"""Manifest parsing, checksum verification, and cache-safe TLC downloads."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.request import urlopen

SERVICES = {"yellow", "green"}
SHA256_RE = re.compile(r"[0-9a-f]{64}")
CHUNK_SIZE = 1024 * 1024


class ManifestError(ValueError):
    """Raised when a manifest is missing, malformed, or inconsistent."""


class ChecksumError(ValueError):
    """Raised when a downloaded file does not match its manifest contract."""


@dataclass(frozen=True)
class TlcFile:
    """One immutable TLC source file in the manifest."""

    service: str
    year: int
    month: int
    url: str
    path: str
    size_bytes: int
    sha256: str
    rows: int | None = None

    @property
    def key(self) -> tuple[str, int, int]:
        """Return the stable service/period identity for this file."""
        return (self.service, self.year, self.month)


def _require_int(value: object, field: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ManifestError(f"{field} must be an integer >= {minimum}")
    return value


def _parse_entry(raw: object) -> TlcFile:
    if not isinstance(raw, dict):
        raise ManifestError("each manifest entry must be an object")

    required = {"service", "year", "month", "url", "path", "size_bytes", "sha256"}
    missing = required - raw.keys()
    if missing:
        raise ManifestError(f"manifest entry is missing {sorted(missing)}")

    service = raw["service"]
    if service not in SERVICES:
        raise ManifestError(f"unsupported service: {service!r}")
    year = _require_int(raw["year"], "year", minimum=2000)
    month = _require_int(raw["month"], "month")
    if month > 12:
        raise ManifestError("month must be between 1 and 12")

    url = raw["url"]
    if not isinstance(url, str) or not url:
        raise ManifestError("url must be a non-empty string")

    path_value = raw["path"]
    if not isinstance(path_value, str) or not path_value:
        raise ManifestError("path must be a non-empty string")
    path = Path(path_value)
    if path.is_absolute() or ".." in path.parts:
        raise ManifestError("path must stay inside the project directory")

    size_bytes = _require_int(raw["size_bytes"], "size_bytes")
    sha256 = raw["sha256"]
    if not isinstance(sha256, str) or not SHA256_RE.fullmatch(sha256):
        raise ManifestError("sha256 must be 64 lowercase hexadecimal characters")

    rows_value = raw.get("rows")
    rows = None if rows_value is None else _require_int(rows_value, "rows", minimum=0)

    return TlcFile(
        service=str(service),
        year=year,
        month=month,
        url=url,
        path=path.as_posix(),
        size_bytes=size_bytes,
        sha256=sha256,
        rows=rows,
    )


def load_manifest(path: Path) -> tuple[TlcFile, ...]:
    """Load and validate a versioned TLC manifest."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ManifestError(f"manifest not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"manifest is not valid JSON: {exc}") from exc

    if not isinstance(data, dict) or data.get("version") != 1:
        raise ManifestError("manifest must contain version: 1")
    raw_files = data.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raise ManifestError("manifest files must be a non-empty list")

    files = tuple(_parse_entry(raw) for raw in raw_files)
    keys = [item.key for item in files]
    if len(keys) != len(set(keys)):
        raise ManifestError("manifest contains a duplicate service/period entry")
    return tuple(sorted(files, key=lambda item: item.key))


def select_file(
    files: tuple[TlcFile, ...], service: str, year: int, month: int
) -> TlcFile:
    """Return the single manifest entry for a service and month."""
    key = (service, year, month)
    for item in files:
        if item.key == key:
            return item
    raise ManifestError(f"manifest entry not found: {service} {year}-{month:02d}")


def resolve_path(root: Path, entry: TlcFile) -> Path:
    """Resolve a manifest path relative to the project root."""
    return root / entry.path


def sha256_file(path: Path) -> str:
    """Hash a local file using the manifest's checksum algorithm."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_file(entry: TlcFile, root: Path) -> Path:
    """Verify size and SHA-256 for an existing local TLC file."""
    path = resolve_path(root, entry)
    if not path.is_file():
        raise FileNotFoundError(f"TLC file not found: {path}; run make seed")
    size = path.stat().st_size
    if size != entry.size_bytes:
        raise ChecksumError(
            f"size mismatch for {path}: expected {entry.size_bytes}, got {size}"
        )
    actual = sha256_file(path)
    if actual != entry.sha256:
        raise ChecksumError(
            f"sha256 mismatch for {path}: expected {entry.sha256}, got {actual}"
        )
    return path


def download_file(entry: TlcFile, root: Path, *, timeout: float = 60.0) -> Path:
    """Download a TLC file into the local cache after verifying its checksum."""
    destination = resolve_path(root, entry)
    if destination.is_file():
        try:
            return verify_file(entry, root)
        except ChecksumError:
            pass

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    digest = hashlib.sha256()
    size = 0

    try:
        with urlopen(entry.url, timeout=timeout) as response, temporary.open("wb") as handle:
            while chunk := response.read(CHUNK_SIZE):
                digest.update(chunk)
                size += len(chunk)
                handle.write(chunk)

        if size != entry.size_bytes:
            raise ChecksumError(
                f"downloaded size mismatch for {entry.url}: "
                f"expected {entry.size_bytes}, got {size}"
            )
        actual = digest.hexdigest()
        if actual != entry.sha256:
            raise ChecksumError(
                f"downloaded sha256 mismatch for {entry.url}: "
                f"expected {entry.sha256}, got {actual}"
            )
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    return destination
