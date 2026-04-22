from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from rent_collector.collectors.base import BaseCollector
from rent_collector.models import DataSource, RentObservation, RoomGroup
from rent_collector.pipeline import run_pipeline
from tests.helpers import make_crosswalk

pytestmark = pytest.mark.integration


class _StubCollector(BaseCollector):
    def __init__(self, observations: list[RentObservation], *, dry_run: bool = False) -> None:
        super().__init__(dry_run=dry_run)
        self._observations = observations

    def collect(self):
        if self.dry_run:
            return iter([])
        return iter(self._observations)


def _obs(
    *,
    code: str,
    room_group: RoomGroup,
    rent_nis: float,
    source: DataSource,
    median_rent_nis: float | None = None,
    avg_rent_nis: float | None = None,
) -> RentObservation:
    crosswalk = make_crosswalk()
    locality = crosswalk.by_code(code)
    assert locality is not None
    return RentObservation(
        locality_code=locality.code,
        locality_name_he=locality.name_he,
        locality_name_en=locality.name_en,
        room_group=room_group,
        median_rent_nis=median_rent_nis,
        avg_rent_nis=avg_rent_nis,
        rent_nis=rent_nis,
        source=source,
        year=2025,
        quarter=2,
    )


def test_run_pipeline_writes_crosswalk_and_output(monkeypatch, tmp_path: Path) -> None:
    crosswalk = make_crosswalk()
    nadlan_rows = [
        _obs(
            code="5000",
            room_group=RoomGroup.R3_0,
            rent_nis=7999,
            median_rent_nis=7999,
            source=DataSource.NADLAN,
        ),
    ]
    cbs_rows = [
        _obs(
            code="9000",
            room_group=RoomGroup.R3_0,
            rent_nis=3200,
            avg_rent_nis=3200,
            source=DataSource.CBS_TABLE49,
        ),
    ]
    boi_rows = [
        _obs(
            code="5000",
            room_group=RoomGroup.R4_0,
            rent_nis=8427,
            source=DataSource.BOI_HEDONIC,
        ),
        _obs(
            code="9000",
            room_group=RoomGroup.R4_0,
            rent_nis=2713,
            source=DataSource.BOI_HEDONIC,
        ),
    ]

    monkeypatch.setattr("rent_collector.pipeline.get_crosswalk", lambda: crosswalk)
    monkeypatch.setattr(
        "rent_collector.pipeline.NadlanCollector",
        lambda dry_run=False: _StubCollector(nadlan_rows, dry_run=dry_run),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSTable49Collector",
        lambda dry_run=False: _StubCollector(cbs_rows, dry_run=dry_run),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSApiCollector",
        lambda dry_run=False, scan_catalog=False: _StubCollector([], dry_run=dry_run),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.BoIHedonicCollector",
        lambda dry_run=False: _StubCollector(boi_rows, dry_run=dry_run),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.DataGovILCollector",
        lambda dry_run=False: _StubCollector([], dry_run=dry_run),
    )

    output_path = tmp_path / "rent_benchmarks.csv"
    crosswalk_path = tmp_path / "locality_crosswalk.csv"
    df = run_pipeline(
        dry_run=False,
        validate=True,
        expected_total_2022=1000,
        output_path=output_path,
        crosswalk_path=crosswalk_path,
    )

    assert output_path.exists()
    assert crosswalk_path.exists()
    assert len(df) == 4

    written = pd.read_csv(output_path)
    assert set(written["source"]) == {
        DataSource.NADLAN.value,
        DataSource.CBS_TABLE49.value,
        DataSource.BOI_HEDONIC.value,
    }
    assert written.loc[written["locality_code"].astype(str) == "5000", "rent_nis"].max() == 8427

    crosswalk_written = pd.read_csv(crosswalk_path)
    assert list(crosswalk_written.columns) == [
        "locality_code",
        "locality_name_he",
        "locality_name_en",
        "district_he",
        "district_en",
        "population_approx",
        "source",
    ]


def test_run_pipeline_dry_run_returns_empty_without_writing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("rent_collector.pipeline.get_crosswalk", make_crosswalk)
    monkeypatch.setattr(
        "rent_collector.pipeline.NadlanCollector",
        lambda dry_run=False: _StubCollector([], dry_run=dry_run),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSTable49Collector",
        lambda dry_run=False: _StubCollector([], dry_run=dry_run),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSApiCollector",
        lambda dry_run=False, scan_catalog=False: _StubCollector([], dry_run=dry_run),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.BoIHedonicCollector",
        lambda dry_run=False: _StubCollector([], dry_run=dry_run),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.DataGovILCollector",
        lambda dry_run=False: _StubCollector([], dry_run=dry_run),
    )

    output_path = tmp_path / "rent_benchmarks.csv"
    crosswalk_path = tmp_path / "locality_crosswalk.csv"
    df = run_pipeline(
        dry_run=True,
        output_path=output_path,
        crosswalk_path=crosswalk_path,
    )

    assert df.empty
    assert not output_path.exists()
    assert not crosswalk_path.exists()
