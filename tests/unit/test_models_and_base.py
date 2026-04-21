from __future__ import annotations

import pytest

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


def test_room_group_from_float_rounds_and_caps() -> None:
    assert RoomGroup.from_float(2.24) == RoomGroup.R2_0
    assert RoomGroup.from_float(2.26) == RoomGroup.R2_5
    assert RoomGroup.from_float(7.0) == RoomGroup.R5_PLUS
    assert RoomGroup.from_float(0.5) == RoomGroup.R5_PLUS
    assert RoomGroup.from_float(float("nan")) == RoomGroup.R5_PLUS


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
