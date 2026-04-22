from __future__ import annotations

import json
from pathlib import Path

from rent_collector import __version__
from rent_collector.config import ROOT_DIR
from rent_collector.pipeline import run_pipeline
from rent_collector.provenance import write_manifest, write_source_inventory_csv

PUBLIC_BUNDLE_DIR = ROOT_DIR / "data" / "public_bundle"
PUBLIC_RENT_BENCHMARKS_CSV = PUBLIC_BUNDLE_DIR / "rent_benchmarks.csv"
PUBLIC_LOCALITY_CROSSWALK_CSV = PUBLIC_BUNDLE_DIR / "locality_crosswalk.csv"
PUBLIC_SOURCE_INVENTORY_CSV = PUBLIC_BUNDLE_DIR / "source_inventory.csv"
PUBLIC_MANIFEST_JSON = PUBLIC_BUNDLE_DIR / "manifest.json"


def build_public_bundle(
    *,
    sources: list[str] | None = None,
    validate: bool = True,
) -> dict[str, object]:
    PUBLIC_BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
    df = run_pipeline(
        sources=sources,
        dry_run=False,
        validate=validate,
        output_path=PUBLIC_RENT_BENCHMARKS_CSV,
        crosswalk_path=PUBLIC_LOCALITY_CROSSWALK_CSV,
    )
    write_source_inventory_csv(PUBLIC_SOURCE_INVENTORY_CSV)
    row_counts = {
        PUBLIC_RENT_BENCHMARKS_CSV.name: len(df.index),
        PUBLIC_LOCALITY_CROSSWALK_CSV.name: _csv_row_count(PUBLIC_LOCALITY_CROSSWALK_CSV),
        PUBLIC_SOURCE_INVENTORY_CSV.name: _csv_row_count(PUBLIC_SOURCE_INVENTORY_CSV),
    }
    return write_manifest(
        root_dir=ROOT_DIR,
        output_path=PUBLIC_MANIFEST_JSON,
        artifact_paths=[
            PUBLIC_RENT_BENCHMARKS_CSV,
            PUBLIC_LOCALITY_CROSSWALK_CSV,
            PUBLIC_SOURCE_INVENTORY_CSV,
        ],
        row_counts=row_counts,
        collector_version=__version__,
    )


def validate_public_bundle(
    bundle_dir: Path = PUBLIC_BUNDLE_DIR, *, root_dir: Path = ROOT_DIR
) -> list[str]:
    errors: list[str] = []
    resolved_root_dir = root_dir.resolve()
    resolved_bundle_dir = bundle_dir.resolve()
    manifest_path = bundle_dir / "manifest.json"
    if not manifest_path.exists():
        return ["manifest.json is missing"]

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return ["manifest.json is not valid JSON"]
    if not isinstance(manifest, dict):
        return ["manifest.json must contain a top-level object"]

    files = manifest.get("files", [])
    if not isinstance(files, list):
        return ["manifest.json field 'files' must be a list"]
    for file_entry in files:
        if not isinstance(file_entry, dict) or "relative_path" not in file_entry:
            errors.append("manifest.json contains a file entry without relative_path")
            continue
        relative_path = str(file_entry["relative_path"])
        relative_path_obj = Path(relative_path)
        if relative_path_obj.is_absolute():
            errors.append(f"absolute path leaked into manifest: {relative_path}")
            continue
        absolute_path = (root_dir / relative_path_obj).resolve()
        try:
            absolute_path.relative_to(resolved_root_dir)
        except ValueError:
            errors.append(f"path escaped bundle root: {relative_path}")
            continue
        try:
            absolute_path.relative_to(resolved_bundle_dir)
        except ValueError:
            errors.append(f"path escaped bundle directory: {relative_path}")
            continue
        if not absolute_path.exists():
            errors.append(f"missing bundle file: {relative_path}")

    if not (bundle_dir / "source_inventory.csv").exists():
        errors.append("source_inventory.csv is missing")
    if not (bundle_dir / "rent_benchmarks.csv").exists():
        errors.append("rent_benchmarks.csv is missing")
    if not (bundle_dir / "locality_crosswalk.csv").exists():
        errors.append("locality_crosswalk.csv is missing")
    return errors


def _csv_row_count(path: Path) -> int:
    with path.open(encoding="utf-8", newline="") as handle:
        return max(sum(1 for _ in handle) - 1, 0)
