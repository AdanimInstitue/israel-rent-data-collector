from __future__ import annotations

from pathlib import Path

import pandas as pd

from rent_collector.collectors.base import BaseCollector
from rent_collector.collectors.cbs_table49 import _extract_table49_entities
from rent_collector.models import DataSource, RentObservation, RoomGroup
from rent_collector.pipeline import (
    _merge_observations,
    _save_crosswalk,
    _validate,
    probe_all,
    run_pipeline,
)
from tests.helpers import make_crosswalk


class _FailingCollector(BaseCollector):
    def collect(self):
        raise RuntimeError("boom")


def test_merge_observations_prefers_higher_priority_source() -> None:
    observations = [
        RentObservation(
            locality_code="5000",
            locality_name_he="תל אביב - יפו",
            locality_name_en="Tel Aviv - Yafo",
            room_group=RoomGroup.R3_0,
            avg_rent_nis=7100,
            rent_nis=7100,
            source=DataSource.CBS_TABLE49,
            year=2025,
            quarter=2,
        ),
        RentObservation(
            locality_code="5000",
            locality_name_he="תל אביב - יפו",
            locality_name_en="Tel Aviv - Yafo",
            room_group=RoomGroup.R3_0,
            median_rent_nis=7999,
            rent_nis=7999,
            source=DataSource.NADLAN,
            year=2025,
            quarter=2,
        ),
    ]

    merged = _merge_observations(observations)

    assert len(merged) == 1
    assert merged.iloc[0]["source"] == DataSource.NADLAN
    assert merged.iloc[0]["rent_nis"] == 7999


def test_extract_table49_entities_uses_latest_value_column() -> None:
    df = pd.DataFrame(
        [
            [None, None, None],
            [None, None, None],
            [None, None, None],
            [None, 2025, None],
            [None, "I-III", None],
            [None, "Average\nprice ", None],
            [None, None, None],
            ["Big cities", None, None],
            ["Tel Aviv - 5000", None, None],
            ["1-2", 5400, None],
            ["2.5-3", 7000, None],
            ["3.5-4", 8600, None],
            ["4.5-6", 10900, None],
        ]
    )

    extracted = _extract_table49_entities(df, value_col=1, year=2025, quarter=1)

    assert extracted["city"].tolist() == ["Tel Aviv", "Tel Aviv", "Tel Aviv", "Tel Aviv"]
    assert extracted["room_group"].tolist() == ["2.0", "3.0", "4.0", "5+"]
    assert extracted["avg_rent_nis"].tolist() == [5400.0, 7000.0, 8600.0, 10900.0]


def test_save_crosswalk_matches_documented_schema(tmp_path: Path) -> None:
    path = tmp_path / "locality_crosswalk.csv"

    _save_crosswalk(make_crosswalk(), path)

    df = pd.read_csv(path)
    assert list(df.columns) == [
        "locality_code",
        "locality_name_he",
        "locality_name_en",
        "district_he",
        "district_en",
        "population_approx",
        "source",
    ]


def test_validate_reports_informational_baseline_and_bounds(capsys) -> None:
    df = pd.DataFrame(
        [
            {"locality_code": "5000", "source": DataSource.NADLAN.value, "rent_nis": 7999},
            {"locality_code": "9000", "source": DataSource.CBS_TABLE49.value, "rent_nis": 2575},
        ]
    )

    _validate(df, expected_total_2022=131_000_000)
    output = capsys.readouterr().out

    assert "informational only" in output
    assert "not directly comparable" in output
    assert "Rent bounds check passed" in output


def test_run_pipeline_handles_unknown_sources_and_failures(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("rent_collector.pipeline.get_crosswalk", make_crosswalk)
    monkeypatch.setattr(
        "rent_collector.pipeline.NadlanCollector", lambda dry_run=False: _FailingCollector()
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSTable49Collector", lambda dry_run=False: _FailingCollector()
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSApiCollector",
        lambda dry_run=False, scan_catalog=False: _FailingCollector(),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.BoIHedonicCollector", lambda dry_run=False: _FailingCollector()
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.DataGovILCollector", lambda dry_run=False: _FailingCollector()
    )

    df = run_pipeline(
        sources=["unknown-source", "nadlan"],
        output_path=tmp_path / "out.csv",
        crosswalk_path=tmp_path / "crosswalk.csv",
    )

    assert df.empty


def test_run_pipeline_validate_and_dry_run(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("rent_collector.pipeline.get_crosswalk", make_crosswalk)

    observation = RentObservation(
        locality_code="5000",
        locality_name_he="תל אביב - יפו",
        locality_name_en="Tel Aviv - Yafo",
        room_group=RoomGroup.R3_0,
        median_rent_nis=7999,
        source=DataSource.NADLAN,
        year=2025,
        quarter=1,
    )

    class _Collector(BaseCollector):
        def __init__(self, items):
            super().__init__()
            self._items = items

        def collect(self):
            return iter(self._items)

    monkeypatch.setattr(
        "rent_collector.pipeline.NadlanCollector", lambda dry_run=False: _Collector([observation])
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSTable49Collector", lambda dry_run=False: _Collector([])
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSApiCollector",
        lambda dry_run=False, scan_catalog=False: _Collector([]),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.BoIHedonicCollector", lambda dry_run=False: _Collector([])
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.DataGovILCollector", lambda dry_run=False: _Collector([])
    )

    df = run_pipeline(
        sources=["nadlan"],
        dry_run=True,
        validate=True,
        expected_total_2022=131_000_000,
        output_path=tmp_path / "out.csv",
        crosswalk_path=tmp_path / "crosswalk.csv",
    )

    assert len(df) == 1
    assert not (tmp_path / "out.csv").exists()


def test_probe_all_aggregates_statuses(monkeypatch) -> None:
    monkeypatch.setattr(
        "rent_collector.pipeline.NadlanCollector",
        lambda: type("C", (), {"probe": lambda self: {"ok": True}})(),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSApiCollector",
        lambda: type("C", (), {"probe": lambda self: {"ok": False}})(),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.CBSTable49Collector",
        lambda: type("C", (), {"probe": lambda self: {"ok": True}})(),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.BoIHedonicCollector",
        lambda: type("C", (), {"probe": lambda self: {"ok": True}})(),
    )
    monkeypatch.setattr(
        "rent_collector.pipeline.DataGovILCollector",
        lambda: type("C", (), {"probe": lambda self: {"ok": True}})(),
    )

    results = probe_all()

    assert results["nadlan"]["ok"] is True
    assert results["cbs-api"]["ok"] is False
