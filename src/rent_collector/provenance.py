from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from rent_collector.source_registry import list_sources


@dataclass(frozen=True)
class FileArtifact:
    relative_path: str
    sha256: str
    bytes: int
    rows: int | None


def write_source_inventory_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "source_id",
        "display_name",
        "homepage_url",
        "terms_url",
        "license_url",
        "access_method",
        "public_status",
        "status",
        "attribution_required",
        "citation_text",
        "redistribution_note",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for source in list_sources():
            writer.writerow(
                {
                    "source_id": source.source_id,
                    "display_name": source.display_name,
                    "homepage_url": source.homepage_url,
                    "terms_url": source.terms_url or "",
                    "license_url": source.license_url or "",
                    "access_method": source.access_method,
                    "public_status": source.source_class,
                    "status": source.status,
                    "attribution_required": str(source.attribution_required).lower(),
                    "citation_text": source.citation_text,
                    "redistribution_note": source.redistribution_note,
                }
            )


def build_file_artifact(root_dir: Path, path: Path, *, rows: int | None = None) -> FileArtifact:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    try:
        relative_path = path.relative_to(root_dir).as_posix()
    except ValueError as exc:
        raise ValueError(
            f"Artifact path '{path}' is outside the root directory '{root_dir}'."
        ) from exc
    return FileArtifact(
        relative_path=relative_path,
        sha256=hasher.hexdigest(),
        bytes=path.stat().st_size,
        rows=rows,
    )


def write_manifest(
    *,
    root_dir: Path,
    output_path: Path,
    artifact_paths: list[Path],
    row_counts: dict[str, int],
    collector_version: str,
) -> dict[str, object]:
    files: list[dict[str, object]] = []
    for artifact_path in artifact_paths:
        files.append(
            asdict(
                build_file_artifact(
                    root_dir,
                    artifact_path,
                    rows=row_counts.get(artifact_path.name),
                )
            )
        )
    manifest: dict[str, object] = {
        "dataset_name": "israel-nadlan-data-public-bundle",
        "collector_version": collector_version,
        "generated_at": datetime.now(UTC).isoformat(),
        "schema_version": "1.0.0",
        "source_summary": [source.as_dict() for source in list_sources()],
        "files": files,
    }
    output_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return manifest
