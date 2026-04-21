from __future__ import annotations

from rent_collector.collectors import cbs_api as mod
from rent_collector.collectors.cbs_api import (
    CBSApiCollector,
    _extract_room_group_from_label,
    _normalise_cbs_series,
    _parse_cbs_series,
    _parse_period,
)
from rent_collector.models import DataSource, RoomGroup
from tests.helpers import make_crosswalk


class _Resp:
    def __init__(self, text: str, json_data=None, status_code: int = 200) -> None:
        self.text = text
        self._json_data = json_data
        self.status_code = status_code

    def json(self):
        if self._json_data is None:
            raise ValueError("no json")
        return self._json_data


class _Client:
    def __init__(self, *, responses=None, json_payload=None) -> None:
        self._responses = list(responses or [])
        self._json_payload = json_payload

    def get(self, *_args, **_kwargs):
        return self._responses.pop(0)

    def get_json(self, *_args, **_kwargs):
        return self._json_payload


def test_parse_period_and_room_group_helpers() -> None:
    assert _parse_period("2024-Q4") == (2024, 4)
    assert _parse_period("2024-12") == (2024, 4)
    assert _parse_period("Q3 2024") == (2024, 3)
    assert _parse_period("2024") == (2024, 4)
    assert _parse_period("unknown") == (2025, 4)
    assert _extract_room_group_from_label("Average rent, 3.5 rooms") == RoomGroup.R3_5
    assert _extract_room_group_from_label("five or more rooms") == RoomGroup.R5_PLUS
    assert _extract_room_group_from_label("0.5 rooms") is None


def test_normalise_and_parse_series(monkeypatch) -> None:
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)

    assert _normalise_cbs_series([{"a": 1}]) == [{"a": 1}]
    assert _normalise_cbs_series({"Data": [{"a": 1}]}) == [{"a": 1}]
    assert _normalise_cbs_series({"series": [{"a": 2}]}) == [{"a": 2}]
    assert _normalise_cbs_series("bad") == []

    rows = [
        {
            "period": "2024-12",
            "value": "5100",
            "rooms": "3",
            "localityCode": "5000",
        },
        {
            "Period": "2024",
            "Average": 4200,
            "description": "national average",
        },
    ]

    parsed = list(_parse_cbs_series(rows, "123", "Test"))

    assert parsed[0].source == DataSource.CBS_API
    assert parsed[0].quarter == 4
    assert parsed[0].room_group == RoomGroup.R3_0
    assert parsed[1].locality_code == "NATIONAL"
    assert parsed[1].notes == "CBS series 123; room group not in data"

    parsed_by_name = list(
        _parse_cbs_series(
            [
                {
                    "period": "2024-Q2",
                    "average": "5300",
                    "city": "תל אביב - יפו",
                    "description": "3 rooms",
                },
                {
                    "period": "2024-Q3",
                    "value": "oops",
                    "cityCode": "5000",
                    "rooms": "3",
                },
            ],
            "124",
            "By name",
        )
    )
    assert len(parsed_by_name) == 1
    assert parsed_by_name[0].locality_code == "5000"


def test_collector_scan_fetch_collect_and_probe(monkeypatch) -> None:
    catalog_json = {
        "chapters": [
            {"chapterId": "4", "chapterName": "Average monthly prices of rent", "mainCode": "40010"}
        ]
    }
    chapter_xml = (
        "<root><index code='150230'><index_name>Rent by 3 rooms</index_name></index></root>"
    )
    probe_resp = _Resp("catalog", status_code=200)
    client = _Client(
        responses=[_Resp("", json_data=catalog_json), _Resp(chapter_xml), probe_resp],
        json_payload={
            "data": [{"period": "2024-Q4", "value": 5000, "localityCode": "5000", "rooms": "3"}]
        },
    )
    monkeypatch.setattr(mod, "get_client", lambda: client)
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)
    monkeypatch.setattr(mod, "CBS_RENT_SERIES", {"123": "Rent"})

    collector = CBSApiCollector()
    matches = collector.scan_catalog()
    series = collector.fetch_series("123")
    collected = list(collector.collect())
    probe = collector.probe()

    assert any(match["id"] == "40010" for match in matches)
    assert series[0]["period"] == "2024-Q4"
    assert collected[0].locality_code == "5000"
    assert probe["ok"] is True


def test_scan_catalog_handles_xml_fallback_and_missing_table49(monkeypatch) -> None:
    catalog_xml = """<root><series id='40010' name='Housing chapter' chapter='4' /></root>"""
    chapter_xml = """<root><index code='777'><index_name>Rent special</index_name></index></root>"""
    client = _Client(responses=[_Resp(catalog_xml), _Resp(chapter_xml)])
    monkeypatch.setattr(mod, "get_client", lambda: client)

    matches = CBSApiCollector().scan_catalog()

    assert any(match["id"] == "40010" for match in matches)
    assert any(match["id"] == "777" for match in matches)


def test_cbs_api_error_and_branch_paths(monkeypatch) -> None:
    class _FailingClient:
        def get(self, *_args, **_kwargs):
            raise RuntimeError("offline")

        def get_json(self, *_args, **_kwargs):
            return {"unexpected": True}

    monkeypatch.setattr(mod, "get_client", lambda: _FailingClient())
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)
    monkeypatch.setattr(mod, "CBS_RENT_SERIES", {})

    collector = CBSApiCollector(dry_run=True, scan_catalog=True)
    assert collector.scan_catalog() == []
    assert list(collector.collect()) == []
    assert collector.probe()["ok"] is False

    xml_client = _Client(responses=[_Resp("<bad", json_data=None)])
    monkeypatch.setattr(mod, "get_client", lambda: xml_client)
    assert CBSApiCollector().scan_catalog() == []

    monkeypatch.setattr(mod, "CBS_RENT_SERIES", {"123": "Rent"})
    monkeypatch.setattr(
        CBSApiCollector,
        "fetch_series",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert list(CBSApiCollector().collect()) == []

    monkeypatch.setattr(mod, "CBS_RENT_SERIES", {})
    assert list(CBSApiCollector().collect()) == []

    parsed = list(
        _parse_cbs_series(
            [
                {"period": "2024-Q1", "value": 0, "city": "missing"},
                {
                    "period": "2024-Q2",
                    "value": 5000,
                    "city": "תל אביב - יפו",
                    "description": "3 rooms",
                },
            ],
            "123",
            "Test",
        )
    )
    assert parsed[0].locality_code == "5000"


def test_scan_catalog_parses_price_all_json(monkeypatch) -> None:
    catalog_json = {
        "chapters": [{"chapterId": "4", "chapterName": "Rent chapter", "mainCode": "40010"}]
    }
    chapter_json = {"data": [{"code": "555", "index_name": "Average monthly prices of rent"}]}
    client = _Client(
        responses=[_Resp("", json_data=catalog_json), _Resp("", json_data=chapter_json)]
    )
    monkeypatch.setattr(mod, "get_client", lambda: client)

    matches = CBSApiCollector().scan_catalog()

    assert any(match["id"] == "555" for match in matches)


def test_scan_catalog_skips_bad_chapters_and_price_all_failures(monkeypatch) -> None:
    catalog_json = {
        "chapters": [
            {"chapterName": "rent chapter", "mainCode": "40010"},
            {"chapterId": "9", "chapterName": "other", "mainCode": "9"},
        ]
    }

    class _ChapterClient(_Client):
        def get(self, *_args, **_kwargs):
            response = self._responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response

    client = _ChapterClient(
        responses=[
            _Resp("", json_data=catalog_json),
            RuntimeError("price_all boom"),
        ]
    )
    monkeypatch.setattr(mod, "get_client", lambda: client)

    matches = CBSApiCollector().scan_catalog()

    assert matches == [{"id": "40010", "name": "rent chapter", "chapter": ""}]
