from __future__ import annotations

from rent_collector.collectors import boi_hedonic as mod
from rent_collector.collectors.boi_hedonic import (
    BoIHedonicCollector,
    _mean_city_effect,
    _room_group_to_float,
)
from rent_collector.models import BoIHedonicCoefficients, RoomGroup
from tests.helpers import make_crosswalk


def test_boi_helpers_predict_collect_download_and_probe(monkeypatch) -> None:
    coeffs = BoIHedonicCoefficients(
        intercept=6.0,
        beta_rooms=0.1,
        city_effects={"5000": 0.0, "9000": -0.2},
    )
    collector = BoIHedonicCollector(coefficients=coeffs, known_localities={"5000"})
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)

    assert _room_group_to_float(RoomGroup.R5_PLUS) == 6.0
    assert _mean_city_effect(coeffs) == -0.1
    assert collector.predict("9000", RoomGroup.R3_0) > 0

    rows = list(collector.collect())
    assert all(row.locality_code == "9000" for row in rows)
    assert len(rows) == 8

    class _Client:
        def get_bytes(self, _url):
            return b"pdf"

    monkeypatch.setattr("rent_collector.utils.http_client.get_client", lambda: _Client())
    assert BoIHedonicCollector.download_paper() == b"pdf"
    assert collector.probe()["ok"] is True


def test_boi_dry_run_placeholder_and_failures(monkeypatch) -> None:
    collector = BoIHedonicCollector()
    monkeypatch.setattr(mod, "COEFFICIENTS_ARE_PLACEHOLDER", True)
    dry_run = BoIHedonicCollector(dry_run=True)
    assert list(dry_run.collect()) == []

    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)
    monkeypatch.setattr(
        collector, "predict", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad"))
    )
    assert list(collector.collect()) == []

    class _FailingClient:
        def get_bytes(self, _url):
            raise RuntimeError("offline")

    monkeypatch.setattr("rent_collector.utils.http_client.get_client", lambda: _FailingClient())
    assert BoIHedonicCollector.download_paper() is None

    monkeypatch.setattr(
        collector, "predict", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    assert collector.probe()["ok"] is False

    assert _mean_city_effect(BoIHedonicCoefficients(intercept=1.0, beta_rooms=0.1)) == -0.2
