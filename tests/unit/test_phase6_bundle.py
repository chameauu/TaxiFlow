"""Unit tests for the committed Superset dashboard bundle builder."""

from __future__ import annotations

import importlib.util
import os
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BUILD_BUNDLE_PATH = PROJECT_ROOT / "config" / "superset" / "build_bundle.py"


def load_build_bundle() -> object:
    spec = importlib.util.spec_from_file_location("taxiflow_build_bundle", BUILD_BUNDLE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_bundle_accepts_files_with_pre_1980_timestamps(tmp_path: Path) -> None:
    """Exported Superset YAML files often carry a 1980-01-01 mtime or older."""
    source = tmp_path / "bundle"
    source.mkdir()
    metadata = source / "metadata.yaml"
    metadata.write_text("version: 1.0.0\n")
    os.utime(metadata, (0, 0))

    destination = tmp_path / "bundle.zip"
    load_build_bundle().build_bundle(source, destination)

    with zipfile.ZipFile(destination) as archive:
        assert archive.namelist() == ["taxiflow-dashboard/metadata.yaml"]
        assert archive.getinfo("taxiflow-dashboard/metadata.yaml").date_time == (
            1980,
            1,
            1,
            0,
            0,
            0,
        )


def test_build_bundle_is_deterministic(tmp_path: Path) -> None:
    source = tmp_path / "bundle"
    source.mkdir()
    (source / "b.yaml").write_text("b: 2\n")
    (source / "a.yaml").write_text("a: 1\n")

    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    module = load_build_bundle()
    module.build_bundle(source, first)
    module.build_bundle(source, second)

    assert first.read_bytes() == second.read_bytes()
    with zipfile.ZipFile(first) as archive:
        assert archive.namelist() == [
            "taxiflow-dashboard/a.yaml",
            "taxiflow-dashboard/b.yaml",
        ]
