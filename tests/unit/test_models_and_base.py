from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from rent_collector import config as config_mod
from rent_collector.collectors.base import BaseCollector
from rent_collector.models import DataSource, RentObservation, RoomGroup


class _Collector(BaseCollector):
    def __init__(self, items: list[RentObservation] | Exception | None) -> None:
        super().__init__()
        self._items = items

    def collect(self):
        if isinstance(self._items, Exception):
            raise self._items
        return iter(self._items or [])


class _DelegatingCollector(BaseCollector):
    def collect(self):
        return super().collect()


def test_room_group_from_float_rounds_and_caps() -> None:
    assert RoomGroup.from_float(2.24) == RoomGroup.R2_0
    assert RoomGroup.from_float(2.26) == RoomGroup.R2_5
    assert RoomGroup.from_float(7.0) == RoomGroup.R5_PLUS

    with pytest.raises(ValueError, match="out of supported range"):
        RoomGroup.from_float(0.5)

    with pytest.raises(ValueError, match="cannot be NaN"):
        RoomGroup.from_float(float("nan"))


def test_rent_observation_prefers_median_then_average() -> None:
    median_obs = RentObservation(
        locality_code="5000",
        locality_name_he="תל אביב - יפו",
        room_group=RoomGroup.R3_0,
        median_rent_nis=7000,
        source=DataSource.NADLAN,
    )
    avg_obs = RentObservation(
        locality_code="5000",
        locality_name_he="תל אביב - יפו",
        room_group=RoomGroup.R3_0,
        avg_rent_nis=7200,
        source=DataSource.CBS_TABLE49,
    )

    assert median_obs.rent_nis == 7000
    assert avg_obs.rent_nis == 7200


def test_rent_observation_requires_median_or_average() -> None:
    with pytest.raises(
        ValueError, match="At least one of median_rent_nis or avg_rent_nis must be set"
    ):
        RentObservation(
            locality_code="5000",
            locality_name_he="תל אביב - יפו",
            room_group=RoomGroup.R3_0,
            source=DataSource.CBS_TABLE49,
        )


def test_base_probe_returns_sample_for_success() -> None:
    collector = _Collector(
        [
            RentObservation(
                locality_code="5000",
                locality_name_he="תל אביב - יפו",
                room_group=RoomGroup.R3_0,
                avg_rent_nis=7200,
                source=DataSource.CBS_TABLE49,
            )
        ]
    )

    result = collector.probe()

    assert result["ok"] is True
    assert result["sample"]["locality_code"] == "5000"


def test_base_probe_handles_empty_and_error() -> None:
    assert _Collector([]).probe()["note"] == "no data returned"
    assert _Collector(RuntimeError("boom")).probe() == {"ok": False, "error": "boom"}


def test_base_collect_abstract_stub_is_reachable() -> None:
    assert _DelegatingCollector().collect() is None


def test_config_detects_repo_root_from_cwd(monkeypatch, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "src" / "rent_collector").mkdir(parents=True)
    (repo / "data").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[project]\nname='demo'\n", encoding="utf-8")
    (repo / "data" / "locality_codes_seed.csv").write_text(
        "locality_code,locality_name_he\n5000,תל אביב - יפו\n", encoding="utf-8"
    )

    monkeypatch.delenv("RENT_COLLECTOR_ROOT_DIR", raising=False)
    monkeypatch.chdir(repo / "src" / "rent_collector")

    reloaded = importlib.reload(config_mod)
    try:
        assert reloaded.ROOT_DIR == repo.resolve()
    finally:
        importlib.reload(config_mod)
