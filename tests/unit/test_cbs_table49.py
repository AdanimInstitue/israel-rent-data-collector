from __future__ import annotations

import pandas as pd
import pytest

from rent_collector.collectors import cbs_table49 as mod
from rent_collector.collectors.cbs_table49 import (
    CBSTable49Collector,
    _clean_table49_label,
    _extract_table49_entities,
    _latest_price_column,
    _resolve_table49_location,
)
from rent_collector.models import RoomGroup
from tests.helpers import make_crosswalk


def test_latest_column_extract_clean_and_resolve(monkeypatch) -> None:
    df = pd.DataFrame(
        [
            [None, None, None, None],
            [None, None, None, None],
            [None, None, None, None],
            [None, 2024, None, 2025],
            [None, "X-XII", None, "I-III"],
            [None, "Average", None, "Average"],
            [None, None, None, None],
            ["Big cities", 1, None, 1],
            ["Tel Aviv - 5000", None, None, None],
            ["1-2", 5000, None, 5400],
            ["2.5-3", 6500, None, 7000],
        ]
    )
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)

    assert _latest_price_column(df) == (3, 2025, 1)
    extracted = _extract_table49_entities(df, value_col=3, year=2025, quarter=1)
    assert extracted["avg_rent_nis"].tolist() == [5400.0, 7000.0]
    assert _clean_table49_label("Petah Tiqwa - 7900") == "Petah Tiqwa"
    assert _resolve_table49_location("Tel Aviv - 5000") == ("5000", "Tel Aviv - Yafo")
    assert _resolve_table49_location("South District") == ("DIST_SOUTH", "South District")


def test_collector_download_parse_collect_and_probe(monkeypatch) -> None:
    collector = CBSTable49Collector()
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)

    class _Resp:
        def __init__(self, status_code: int, content: bytes = b"x") -> None:
            self.status_code = status_code
            self.content = content

    class _Client:
        def __init__(self, responses):
            self._responses = list(responses)

        def get(self, *_args, **_kwargs):
            return self._responses.pop(0)

    monkeypatch.setattr(mod, "get_client", lambda: _Client([_Resp(200, b"excel")]))
    assert collector._download_excel() == b"excel"

    monkeypatch.setattr(mod, "get_client", lambda: _Client([_Resp(404, b"missing")]))
    assert collector._download_excel() is None

    monkeypatch.setattr(collector, "_download_excel", lambda: None)
    monkeypatch.setattr(collector, "_download_pdf", lambda: b"pdf")
    assert collector.probe()["format"] == "pdf"

    monkeypatch.setattr(collector, "_download_excel", lambda: b"excel")
    monkeypatch.setattr(
        collector,
        "_parse_excel",
        lambda _content: pd.DataFrame(
            [
                {
                    "city": "תל אביב - יפו",
                    "room_group": RoomGroup.R3_0.value,
                    "avg_rent_nis": 7999.0,
                    "year": 2025,
                    "quarter": 1,
                }
            ]
        ),
    )
    rows = list(collector.collect())
    assert rows[0].avg_rent_nis == 7999.0

    class _Page:
        def extract_table(self):
            return None

    class _Pdf:
        pages = [_Page()]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr("pdfplumber.open", lambda *_args, **_kwargs: _Pdf())
    with pytest.raises(ValueError, match="No table found"):
        collector._parse_pdf(b"pdf")

    class _PdfWithTable:
        pages = [type("Page", (), {"extract_table": lambda self: [["a"]]})()]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr("pdfplumber.open", lambda *_args, **_kwargs: _PdfWithTable())
    with pytest.raises(ValueError, match="not implemented"):
        collector._parse_pdf(b"pdf")


def test_cbs_table49_branch_paths(monkeypatch) -> None:
    collector = CBSTable49Collector(dry_run=True)
    assert list(collector.collect()) == []

    collector = CBSTable49Collector()
    monkeypatch.setattr(collector, "_download_excel", lambda: None)
    monkeypatch.setattr(collector, "_download_pdf", lambda: None)
    assert list(collector.collect()) == []
    assert collector.probe()["ok"] is False

    monkeypatch.setattr(collector, "_download_excel", lambda: b"excel")
    monkeypatch.setattr(
        collector, "_parse_excel", lambda _content: (_ for _ in ()).throw(ValueError("bad"))
    )
    assert list(collector.collect()) == []

    df = pd.DataFrame(
        [
            [None, None],
            [None, None],
            [None, None],
            [None, 2025],
            [None, "I-III"],
            [None, "Average"],
            [None, None],
            ["Big cities", 1],
            ["Total", None],
            ["(note)", None],
            ["City", None],
            ["1-2", "-"],
        ]
    )
    assert _extract_table49_entities(df, value_col=1, year=2025, quarter=1).empty
    with pytest.raises(ValueError):
        _latest_price_column(
            pd.DataFrame([[None], [None], [None], [None], [None], [None], [None], [None]])
        )
    assert _resolve_table49_location("Unknown City") == ("UNKNOWN_Unknown City", "Unknown City")


def test_cbs_table49_parser_and_lookup_branches(monkeypatch) -> None:
    collector = CBSTable49Collector()
    monkeypatch.setattr(mod, "get_crosswalk", make_crosswalk)

    class _ErrorClient:
        def get(self, *_args, **_kwargs):
            raise RuntimeError("offline")

    monkeypatch.setattr(mod, "get_client", lambda: _ErrorClient())
    assert collector._download_pdf() is None

    fake_df = pd.DataFrame(
        [
            [None, None, None],
            [None, None, None],
            [None, None, None],
            [None, 2024, 2025],
            [None, "Annual\naverage", "I-III"],
            [None, "Average", "Average"],
            [None, None, None],
            [None, 1, 1],
        ]
    )
    monkeypatch.setattr("pandas.read_excel", lambda *_args, **_kwargs: fake_df)
    assert collector._parse_excel(b"excel").empty

    df = pd.DataFrame(
        [
            [None, None, None],
            [None, None, None],
            [None, None, None],
            [None, 2024, None],
            [None, "X-XII", "BAD"],
            [None, "Average", "Average"],
            [None, None, None],
            ["Residential Districts", 1, None],
            ["Tel Aviv - 5000", None, None],
            ["1-2", 5000, None],
            ["Total", None, None],
            ["2.5-3", 7000, None],
            ["South District", None, None],
            ["4.5-6", "-", 9000],
        ]
    )

    assert _latest_price_column(df) == (1, 2024, 4)
    extracted = _extract_table49_entities(df, value_col=1, year=2024, quarter=4)
    assert extracted["city"].tolist() == ["Tel Aviv"]
    assert _resolve_table49_location("תל אביב - יפו - 9999") == ("5000", "Tel Aviv - Yafo")
