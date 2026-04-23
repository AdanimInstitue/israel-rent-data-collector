from __future__ import annotations

import json

import pytest

from rent_collector.models import Locality
from rent_collector.public_bundle import build_public_bundle, validate_public_bundle
from rent_collector.utils.locality_crosswalk import LocalityCrosswalk


@pytest.mark.integration
def test_build_and_validate_public_bundle(monkeypatch, tmp_path) -> None:
    bundle_dir = tmp_path / "public_bundle"
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

    crosswalk = LocalityCrosswalk(
        [Locality(code="5000", name_he="תל אביב - יפו", district_he="תל אביב")]
    )
    monkeypatch.setattr("rent_collector.pipeline.get_crosswalk", lambda: crosswalk)

    manifest = build_public_bundle(validate=True)

    assert manifest["files"]
    assert validate_public_bundle(bundle_dir, root_dir=tmp_path) == []
    stored = json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))
    assert stored["dataset_name"] == "israel-nadlan-data-public-reference-bundle"
