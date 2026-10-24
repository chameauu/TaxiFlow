"""Unit tests for the TLC file manifest and checksum contract."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from taxiflow.tlc.manifest import (
    ChecksumError,
    ManifestError,
    download_file,
    load_manifest,
    select_file,
    verify_file,
)

CHECKSUM = "a" * 64


def write_manifest(tmp_path: Path, entries: list[dict[str, object]]) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"version": 1, "files": entries}), encoding="utf-8")
    return path


def entry(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        "service": "yellow",
        "year": 2024,
        "month": 1,
        "url": "https://example.invalid/yellow.parquet",
        "path": "data/raw/yellow.parquet",
        "size_bytes": 4,
        "sha256": CHECKSUM,
        "rows": 1,
    }
    data.update(overrides)
    return data


def test_load_manifest_returns_sorted_entries(tmp_path: Path) -> None:
    manifest = write_manifest(
        tmp_path,
        [
            entry(month=2),
            entry(month=1, service="yellow"),
            entry(service="green", path="data/raw/green.parquet"),
        ],
    )

    files = load_manifest(manifest)

    assert [item.key for item in files] == [
        ("green", 2024, 1),
        ("yellow", 2024, 1),
        ("yellow", 2024, 2),
    ]


def test_load_manifest_rejects_duplicate_keys(tmp_path: Path) -> None:
    manifest = write_manifest(tmp_path, [entry(), entry()])

    with pytest.raises(ManifestError, match="duplicate"):
        load_manifest(manifest)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("service", "fhv"),
        ("month", 13),
        ("size_bytes", 0),
        ("sha256", "not-a-checksum"),
        ("path", "../outside.parquet"),
        ("path", "/absolute.parquet"),
    ],
)
def test_load_manifest_rejects_invalid_entries(
    tmp_path: Path, field: str, value: object
) -> None:
    manifest = write_manifest(tmp_path, [entry(**{field: value})])

    with pytest.raises(ManifestError):
        load_manifest(manifest)


def test_select_file_finds_exact_period(tmp_path: Path) -> None:
    manifest = write_manifest(tmp_path, [entry()])
    files = load_manifest(manifest)

    assert select_file(files, "yellow", 2024, 1).service == "yellow"


def test_select_file_rejects_missing_period(tmp_path: Path) -> None:
    manifest = write_manifest(tmp_path, [entry()])
    files = load_manifest(manifest)

    with pytest.raises(ManifestError, match="not found"):
        select_file(files, "yellow", 2024, 2)


def test_verify_file_accepts_matching_size_and_checksum(tmp_path: Path) -> None:
    payload = b"tlc\n"
    source = tmp_path / "data" / "raw" / "yellow.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(payload)
    manifest = write_manifest(
        tmp_path,
        [
            entry(
                size_bytes=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
            )
        ],
    )

    resolved = verify_file(load_manifest(manifest)[0], tmp_path)

    assert resolved == source


def test_verify_file_rejects_checksum_mismatch(tmp_path: Path) -> None:
    source = tmp_path / "data" / "raw" / "yellow.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"tlc\n")
    manifest = write_manifest(tmp_path, [entry(size_bytes=4, sha256="b" * 64)])

    with pytest.raises(ChecksumError, match="sha256"):
        verify_file(load_manifest(manifest)[0], tmp_path)


def test_download_file_uses_atomic_local_copy(tmp_path: Path) -> None:
    payload = b"fixture-bytes"
    source = tmp_path / "remote.parquet"
    source.write_bytes(payload)
    manifest = write_manifest(
        tmp_path,
        [
            entry(
                url=source.as_uri(),
                path="data/raw/yellow.parquet",
                size_bytes=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
            )
        ],
    )

    resolved = download_file(load_manifest(manifest)[0], tmp_path)

    assert resolved.read_bytes() == payload
    assert not (tmp_path / "data" / "raw" / "yellow.parquet.part").exists()
