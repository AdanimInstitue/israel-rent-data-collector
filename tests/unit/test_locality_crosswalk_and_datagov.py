from __future__ import annotations

import csv

import pytest

from rent_collector.collectors.data_gov_il import (
    DataGovILCollector,
    ckan_datastore_search,
    ckan_organization_datasets,
    ckan_package_search,
)
from rent_collector.models import Locality
from rent_collector.utils import locality_crosswalk as lc
from rent_collector.utils.locality_crosswalk import LocalityCrosswalk


class _Client:
    def __init__(self, payloads):
        self._payloads = list(payloads)

    def get_json(self, *_args, **_kwargs):
        return self._payloads.pop(0)


def test_fetch_from_datagov_populates_district_and_source(monkeypatch) -> None:
    payload = {
        "success": True,
        "result": {
            "records": [
                {
                    "סמל_ישוב": "5000 ",
                    "שם_ישוב": "תל אביב - יפו ",
                    "שם_ישוב_לועזי": "TEL AVIV - YAFO ",
                    "סמל_נפה": 51,
                    "שם_נפה": "תל אביב ",
                    "לשכה": "תל אביב ",
                },
                {
                    "סמל_ישוב": "7000 ",
                    "שם_ישוב": "לוד ",
                    "שם_ישוב_לועזי": "LOD ",
                    "סמל_נפה": 43,
                    "שם_נפה": "רמלה ",
                },
            ]
        },
    }
    monkeypatch.setattr("rent_collector.utils.http_client.get_client", lambda: _Client([payload]))

    localities = lc._fetch_from_datagov()

    assert [loc.code for loc in localities] == ["5000", "7000"]
    assert localities[0].district_he == "תל אביב"
    assert localities[0].district_en == "Tel Aviv"
    assert localities[0].sub_district_he == "תל אביב"
    assert localities[0].source == "data.gov.il"
    assert localities[1].district_he == "המרכז"


def test_fetch_from_datagov_skips_invalid_and_unsuccessful_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        "rent_collector.utils.http_client.get_client", lambda: _Client([{"success": False}])
    )
    with pytest.raises(ValueError, match="success=false"):
        lc._fetch_from_datagov()

    malformed_payload = {
        "success": True,
        "result": {
            "records": [
                {"שם_ישוב": "Missing code"},
                {"סמל_ישוב": "7000", "שם_ישוב": "Bad population", "סה_כ": "oops"},
                {"סמל_ישוב": "9000", "שם_ישוב": "באר שבע"},
            ]
        },
    }
    monkeypatch.setattr(
        "rent_collector.utils.http_client.get_client", lambda: _Client([malformed_payload])
    )

    localities = lc._fetch_from_datagov()

    assert [loc.code for loc in localities] == ["9000"]


def test_load_seed_csv_and_normalized_lookup(monkeypatch, tmp_path) -> None:
    seed_path = tmp_path / "seed.csv"
    with open(seed_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "locality_code",
                "locality_name_he",
                "locality_name_en",
                "district_he",
                "population_approx",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "locality_code": "5000",
                "locality_name_he": "תל אביב - יפו",
                "locality_name_en": "TEL AVIV - YAFO",
                "district_he": "תל אביב",
                "population_approx": "460000",
            }
        )

    monkeypatch.setattr(lc, "SEED_LOCALITIES_CSV", seed_path)

    localities = lc._load_seed_csv()
    crosswalk = LocalityCrosswalk(localities)

    assert localities[0].district_en == "Tel Aviv"
    assert localities[0].source == "seed_csv"
    assert crosswalk.by_code_padded("05000") is not None
    assert crosswalk.by_code_padded("DIST_TA") is None
    assert crosswalk.by_code_padded("") is None
    assert crosswalk.by_name(" תל אביב-יפו ") is not None
    assert crosswalk.by_name_en("tel aviv - yafo") is not None
    assert crosswalk.by_code("05000") is not None
    assert len(crosswalk) == 1
    assert crosswalk.all_codes() == ["5000"]


def test_load_seed_csv_skips_malformed_rows(monkeypatch, tmp_path) -> None:
    seed_path = tmp_path / "seed.csv"
    seed_path.write_text(
        "locality_code,locality_name_he,locality_name_en,district_he,population_approx\n"
        "bad,שם,Name,תל אביב,100\n"
        "5000,תל אביב - יפו,TEL AVIV - YAFO,תל אביב,460000\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(lc, "SEED_LOCALITIES_CSV", seed_path)

    localities = lc._load_seed_csv()

    assert [loc.code for loc in localities] == ["5000"]


def test_crosswalk_load_falls_back_to_seed(monkeypatch) -> None:
    sentinel = LocalityCrosswalk([])
    monkeypatch.setattr(
        lc, "_fetch_from_datagov", lambda: (_ for _ in ()).throw(RuntimeError("offline"))
    )
    monkeypatch.setattr(lc, "_load_seed_csv", lambda: sentinel.all_localities())

    crosswalk = LocalityCrosswalk.load()

    assert len(crosswalk) == 0


def test_crosswalk_load_prefers_live_datagov_results(monkeypatch) -> None:
    monkeypatch.setattr(
        lc,
        "_fetch_from_datagov",
        lambda: [Locality(code="5000", name_he="תל אביב - יפו", district_he="תל אביב")],
    )

    crosswalk = LocalityCrosswalk.load()

    assert len(crosswalk) == 1
    assert crosswalk.by_code("5000") is not None


def test_ckan_helpers_and_collector(monkeypatch) -> None:
    datastore_pages = [
        {"success": True, "result": {"records": [{"a": 1}], "total": 2}},
        {"success": True, "result": {"records": [{"a": 2}], "total": 2}},
    ]
    package_payload = {
        "success": True,
        "result": {
            "results": [{"title": "Rent", "organization": {"name": "cbs"}, "resources": []}, "bad"]
        },
    }
    org_payload = {
        "success": True,
        "result": {"packages": [{"title": "Pkg"}, "bad"]},
    }

    monkeypatch.setattr(
        "rent_collector.collectors.data_gov_il.get_client", lambda: _Client(datastore_pages)
    )
    assert ckan_datastore_search("resource") == [{"a": 1}, {"a": 2}]

    monkeypatch.setattr(
        "rent_collector.collectors.data_gov_il.get_client", lambda: _Client([package_payload])
    )
    assert ckan_package_search("rent") == [
        {"title": "Rent", "organization": {"name": "cbs"}, "resources": []}
    ]

    monkeypatch.setattr(
        "rent_collector.collectors.data_gov_il.get_client", lambda: _Client([org_payload])
    )
    assert ckan_organization_datasets("cbs") == [{"title": "Pkg"}]

    collector = DataGovILCollector()
    monkeypatch.setattr(
        "rent_collector.collectors.data_gov_il.ckan_package_search",
        lambda *_args, **_kwargs: [
            {
                "title": "Rent",
                "organization": {"name": "cbs"},
                "resources": [{"format": "CSV", "name": "dataset", "url": "https://example.com"}],
            }
        ],
    )
    monkeypatch.setattr(
        "rent_collector.collectors.data_gov_il.ckan_datastore_search",
        lambda *_args, **_kwargs: [{"סמל_ישוב": "5000"}],
    )
    assert list(collector.collect()) == []
    assert collector.discover_datasets("rent")[0]["title"] == "Rent"
    assert collector.probe()["ok"] is True


def test_crosswalk_and_datagov_error_branches(monkeypatch, tmp_path) -> None:
    original_load_seed_csv = lc._load_seed_csv

    monkeypatch.setattr(
        "rent_collector.collectors.data_gov_il.get_client",
        lambda: _Client([{"success": False}, {"success": False}]),
    )
    assert ckan_package_search("rent") == []
    assert ckan_organization_datasets("cbs") == []

    monkeypatch.setattr(
        "rent_collector.collectors.data_gov_il.get_client",
        lambda: _Client([{"success": False, "result": {"records": [], "total": 0}}]),
    )
    assert ckan_datastore_search("resource", filters={"a": 1}, q="rent") == []

    collector = DataGovILCollector()
    monkeypatch.setattr(
        "rent_collector.collectors.data_gov_il.ckan_datastore_search",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert collector.probe()["ok"] is False

    monkeypatch.setattr(lc, "_fetch_from_datagov", lambda: [])
    monkeypatch.setattr(lc, "_load_seed_csv", lambda: [])
    assert len(LocalityCrosswalk.load(force_seed=True)) == 0

    monkeypatch.setattr(lc, "_load_seed_csv", original_load_seed_csv)
    missing_seed = tmp_path / "missing.csv"
    monkeypatch.setattr(lc, "SEED_LOCALITIES_CSV", missing_seed)
    try:
        lc._load_seed_csv()
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Expected FileNotFoundError for missing seed CSV")

    assert lc._district_name_he({"שם_מחוז": "תל אביב"}) == "תל אביב"
    assert lc._district_name_he({"סמל_נפה": "bad"}) == ""
    assert lc._district_name_en("לא קיים") == ""


def test_get_crosswalk_uses_cached_loader(monkeypatch) -> None:
    lc.get_crosswalk.cache_clear()
    sentinel = LocalityCrosswalk(
        [Locality(code="5000", name_he="תל אביב - יפו", district_he="תל אביב")]
    )
    monkeypatch.setattr(lc.LocalityCrosswalk, "load", classmethod(lambda cls: sentinel))

    first = lc.get_crosswalk()
    second = lc.get_crosswalk()

    assert first is sentinel
    assert second is sentinel
    lc.get_crosswalk.cache_clear()
