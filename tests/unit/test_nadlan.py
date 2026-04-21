from __future__ import annotations

from rent_collector.collectors import nadlan as mod
from rent_collector.collectors.nadlan import (
    NadlanCollector,
    _base_url_for_pattern,
    _build_params,
    _extract_price,
    _item_to_observation,
    _latest_graph_point,
    _looks_like_rent_data,
    _parse_nextjs_blob,
    _parse_response,
)
from rent_collector.models import DataSource, RoomGroup
from tests.helpers import make_crosswalk


class _Resp:
    def __init__(self, status_code: int, payload) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = "<html></html>"

    def json(self):
        return self._payload


class _Client:
    def __init__(self, responses) -> None:
        self._responses = list(responses)

    def get(self, *_args, **_kwargs):
        return self._responses.pop(0)


def test_nadlan_helpers_and_parsers() -> None:
    assert _base_url_for_pattern("PROXY:/api/test").startswith("https://proxy-nadlan")
    assert _build_params("5000", "/api/getRentsBySettlement")["id"] == "5000"
    assert _extract_price({"avg": "7"}, ["avg"]) == 7.0
    assert _extract_price({"avg": 0}, ["avg"]) is None
    point = _latest_graph_point(
        [
            {"year": 2024, "month": 3, "settlementPrice": 7000},
            {"year": 2025, "month": 1, "settlementPrice": 7999},
        ]
    )
    assert point == {"year": 2025, "month": 1, "settlementPrice": 7999}

    live_rows = list(
        _parse_response(
            {
                "trends": {
                    "rooms": [
                        {
                            "numRooms": 3,
                            "summary": {"lastYearAvgPrice": 7999},
                            "graphData": [
                                {"year": 2024, "month": 1, "settlementPrice": 7000},
                                {"year": 2025, "month": 2, "settlementPrice": 7999},
                            ],
                        }
                    ]
                }
            },
            "5000",
            "תל אביב - יפו",
            "Tel Aviv - Yafo",
        )
    )
    assert live_rows[0].avg_rent_nis == 7999
    assert live_rows[0].median_rent_nis is None
    assert live_rows[0].quarter == 1

    shape_c = list(
        _parse_response(
            {"rentByRooms": {"3": {"median": 7000, "count": 12}}},
            "5000",
            "תל אביב - יפו",
            "Tel Aviv - Yafo",
        )
    )
    assert shape_c[0].median_rent_nis == 7000
    assert shape_c[0].observations_count == 12

    nextjs = list(
        _parse_nextjs_blob(
            {"props": {"pageProps": {"rentData": {"rentByRooms": {"4": {"avg": 8500}}}}}},
            "5000",
            "תל אביב - יפו",
            "Tel Aviv - Yafo",
        )
    )
    assert nextjs[0].room_group == RoomGroup.R4_0

    item = _item_to_observation(
        {"rooms": "2.5", "median": 5400, "quarter": 2, "year": 2025, "count": 5},
        "5000",
        "תל אביב - יפו",
        "Tel Aviv - Yafo",
        2025,
        1,
    )
    assert item is not None
    assert item.room_group == RoomGroup.R2_5
    assert item.observations_count == 5
    assert _looks_like_rent_data({"trends": {"rooms": []}}) is True
    assert _looks_like_rent_data("short") is False


def test_nadlan_collector_discovery_collect_and_probe(monkeypatch) -> None:
    payload = {
        "trends": {
            "rooms": [
                {
                    "numRooms": 3,
                    "summary": {"lastYearAvgPrice": 7999},
                    "graphData": [{"year": 2025, "month": 2, "settlementPrice": 7900}],
                }
            ]
        }
    }
    client = _Client([_Resp(200, payload), _Resp(200, payload), _Resp(200, payload)])
    monkeypatch.setattr("rent_collector.utils.http_client.get_client", lambda: client)
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)

    collector = NadlanCollector(locality_codes=["5000"])
    endpoint = collector.discover_endpoint()
    rows = list(collector.collect())
    probe = collector.probe()

    assert endpoint is not None
    assert rows[0].source == DataSource.NADLAN
    assert rows[0].year == 2025
    assert rows[0].quarter == 2
    assert probe["ok"] is True


def test_nadlan_branch_paths(monkeypatch) -> None:
    class _ErrorClient:
        def __init__(self, responses=None, exc=None):
            self._responses = list(responses or [])
            self._exc = exc

        def get(self, *_args, **_kwargs):
            if self._exc is not None:
                raise self._exc
            return self._responses.pop(0)

    collector = NadlanCollector(locality_codes=["5000", "9999"], dry_run=True)
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)
    assert list(collector.collect()) == []

    monkeypatch.setattr(
        mod, "get_client", lambda: _ErrorClient(exc=mod.requests.RequestException("boom"))
    )
    assert collector.discover_endpoint() is None

    collector = NadlanCollector(locality_codes=["5000"])
    collector._active_endpoint = "/api/getRentsBySettlement"
    monkeypatch.setattr(mod, "get_client", lambda: _Client([_Resp(500, {})]))
    assert list(collector._fetch_via_api("5000", "תל אביב - יפו", "Tel Aviv - Yafo")) == []

    class _BadJsonResp(_Resp):
        def json(self):
            raise ValueError("bad")

    monkeypatch.setattr(mod, "get_client", lambda: _Client([_BadJsonResp(200, {})]))
    assert list(collector._fetch_via_api("5000", "תל אביב - יפו", "Tel Aviv - Yafo")) == []

    html = """
    <html><body>
    <script id="__NEXT_DATA__">
    {"props":{"pageProps":{"rentData":{"rentByRooms":{"3":{"median":7000}}}}}}
    </script>
    </body></html>
    """
    monkeypatch.setattr(
        mod, "get_client", lambda: _Client([type("Resp", (), {"status_code": 200, "text": html})()])
    )
    assert list(collector._fetch_via_html("5000", "תל אביב - יפו", "Tel Aviv - Yafo"))

    html = """
    <html><body>
    <script>window.__DATA__ = {\"rentByRooms\":{\"4\":{\"avg\":8500}}};</script>
    </body></html>
    """
    monkeypatch.setattr(
        mod, "get_client", lambda: _Client([type("Resp", (), {"status_code": 200, "text": html})()])
    )
    assert list(collector._fetch_via_html("5000", "תל אביב - יפו", "Tel Aviv - Yafo"))

    monkeypatch.setattr(mod, "get_client", lambda: _ErrorClient(exc=RuntimeError("offline")))
    assert list(collector._fetch_via_html("5000", "תל אביב - יפו", "Tel Aviv - Yafo")) == []

    monkeypatch.setattr(
        mod, "get_client", lambda: _Client([type("Resp", (), {"status_code": 404, "text": ""})()])
    )
    assert list(collector._fetch_via_html("5000", "תל אביב - יפו", "Tel Aviv - Yafo")) == []

    assert list(_parse_response([{"rooms": "x"}], "5000", "תל אביב", "TA")) == []
    assert list(_parse_response({"data": [{"rooms": "3", "avg": 7000}]}, "5000", "תל אביב", "TA"))
    assert list(_parse_response({"rooms": {"3": {"avgRent": 7000}}}, "5000", "תל אביב", "TA"))
    assert list(_parse_response({"unknown": True}, "5000", "תל אביב", "TA")) == []

    assert list(_parse_nextjs_blob({}, "5000", "תל אביב", "TA")) == []
    assert _item_to_observation([], "5000", "תל אביב", "TA", 2025, 1) is None
    assert (
        _item_to_observation({"rooms": "bad", "avg": 1}, "5000", "תל אביב", "TA", 2025, 1) is None
    )
    assert _item_to_observation({"rooms": "3"}, "5000", "תל אביב", "TA", 2025, 1) is None
    assert mod._parse_room_group("חדר 1") == RoomGroup.R1_0
    assert mod._parse_room_group("bad") is None
    assert _extract_price({"avg": "bad"}, ["avg"]) is None
    assert _latest_graph_point([{"year": 2025, "month": 1}, "x"]) is None


def test_nadlan_additional_branch_coverage(monkeypatch, caplog) -> None:
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)

    payload = {
        "trends": {
            "rooms": [
                "bad",
                {"numRooms": "bad", "summary": {"lastYearAvgPrice": 100}},
                {"numRooms": 4, "summary": {}, "graphData": [{"year": 2025, "month": 6}]},
                {
                    "numRooms": 4,
                    "summary": {},
                    "graphData": [{"year": 2025, "month": 6, "settlementPrice": 8500}],
                },
            ]
        }
    }
    parsed = list(_parse_response(payload, "5000", "תל אביב", "TA"))
    assert len(parsed) == 1
    assert parsed[0].avg_rent_nis == 8500
    assert parsed[0].quarter == 2

    assert _build_params("5000", "/NadlanAPI/LegacySettlement") == {
        "settlementCode": "5000",
        "fromDate": "2024-01-01",
        "toDate": "2025-12-31",
    }
    assert _looks_like_rent_data({"note": "rent average price data " * 3}) is True
    assert _latest_graph_point(
        [
            {"year": 2025, "month": 1, "settlementPrice": 7000},
            {"year": 2025, "month": 3, "settlementPrice": 7500},
        ]
    ) == {"year": 2025, "month": 3, "settlementPrice": 7500}

    collector = NadlanCollector(locality_codes=["9999", "5000"])
    collector._active_endpoint = "/api/getRentsBySettlement"
    monkeypatch.setattr(
        collector,
        "_fetch_locality",
        lambda code, *_args: (
            (_ for _ in ()).throw(RuntimeError("boom")) if code == "5000" else iter(())
        ),
    )
    assert list(collector.collect()) == []

    assert any("Unknown locality code" in record.message for record in caplog.records)
    assert any("Failed to fetch locality 5000" in record.message for record in caplog.records)

    room_rows = list(
        _parse_response(
            {"rooms": {"x": [], "3": {"median": None, "avg": None}, "4": {"avg": 8000}}},
            "5000",
            "תל אביב",
            "TA",
        )
    )
    assert len(room_rows) == 1
    assert room_rows[0].room_group == RoomGroup.R4_0
