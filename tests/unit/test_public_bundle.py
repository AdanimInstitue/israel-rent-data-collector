from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from rent_collector import __version__
from rent_collector.provenance import write_manifest, write_source_inventory_csv
from rent_collector.public_bundle import build_public_bundle, validate_public_bundle


def test_validate_public_bundle_detects_missing_manifest(tmp_path: Path) -> None:
    assert validate_public_bundle(tmp_path) == ["manifest.json is missing"]


def test_validate_public_bundle_reports_invalid_manifest_json(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "custom_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text("{", encoding="utf-8")
    assert validate_public_bundle(bundle_dir) == ["manifest.json is not valid JSON"]


def test_write_manifest_uses_relative_paths(tmp_path: Path) -> None:
    root_dir = tmp_path
    bundle_dir = tmp_path / "data" / "public_bundle"
    bundle_dir.mkdir(parents=True)
    inventory_path = bundle_dir / "source_inventory.csv"
    write_source_inventory_csv(inventory_path)

    manifest = write_manifest(
        root_dir=root_dir,
        output_path=bundle_dir / "manifest.json",
        artifact_paths=[inventory_path],
        row_counts={"source_inventory.csv": 1},
        collector_version="0.3.0",
    )

    assert manifest["files"][0]["relative_path"] == "data/public_bundle/source_inventory.csv"


def test_build_public_bundle_writes_manifest_with_package_version(
    tmp_path: Path, monkeypatch
) -> None:
    bundle_dir = tmp_path / "bundle"
    monkeypatch.setattr("rent_collector.public_bundle.PUBLIC_BUNDLE_DIR", bundle_dir)
    monkeypatch.setattr(
        "rent_collector.public_bundle.PUBLIC_LOCALITY_CROSSWALK_CSV",
        bundle_dir / "locality_crosswalk.csv",
    )
    monkeypatch.setattr(
        "rent_collector.public_bundle.PUBLIC_SOURCE_INVENTORY_CSV",
        bundle_dir / "source_inventory.csv",
    )
    monkeypatch.setattr(
        "rent_collector.public_bundle.PUBLIC_MANIFEST_JSON", bundle_dir / "manifest.json"
    )
    monkeypatch.setattr("rent_collector.public_bundle.ROOT_DIR", tmp_path)

    def _run_pipeline(**_: object) -> pd.DataFrame:
        bundle_dir.mkdir(parents=True, exist_ok=True)
        (bundle_dir / "locality_crosswalk.csv").write_text("col\n1\n", encoding="utf-8")
        return pd.DataFrame([{"col": 1}])

    monkeypatch.setattr("rent_collector.public_bundle.run_pipeline", _run_pipeline)

    manifest = build_public_bundle()

    assert manifest["collector_version"] == __version__
    assert (bundle_dir / "manifest.json").exists()


def test_validate_public_bundle_detects_missing_required_files(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "custom_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(
        json.dumps({"files": [{"relative_path": "custom_bundle/locality_crosswalk.csv"}]}),
        encoding="utf-8",
    )

    errors = validate_public_bundle(bundle_dir, root_dir=tmp_path)
    assert "source_inventory.csv is missing" in errors
    assert "locality_crosswalk.csv is missing" in errors


def test_validate_public_bundle_rejects_absolute_and_escaping_paths(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "custom_bundle"
    bundle_dir.mkdir()
    (bundle_dir / "source_inventory.csv").write_text("col\n1\n", encoding="utf-8")
    (bundle_dir / "locality_crosswalk.csv").write_text("col\n1\n", encoding="utf-8")
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "files": [
                    {"relative_path": "../outside.csv"},
                    {"relative_path": "/absolute/path.csv"},
                ]
            }
        ),
        encoding="utf-8",
    )
    errors = validate_public_bundle(bundle_dir, root_dir=tmp_path)
    assert "path escaped bundle root: ../outside.csv" in errors
    assert "absolute path leaked into manifest: /absolute/path.csv" in errors
