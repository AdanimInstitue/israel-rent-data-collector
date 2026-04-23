from __future__ import annotations

import click
import pandas as pd
import pytest

from rent_collector.models import Locality
from rent_collector.pipeline import (
    ValidationFailedError,
    _crosswalk_dataframe,
    _normalize_sources,
    _validate_crosswalk,
    probe_all,
    run_pipeline,
)
from rent_collector.utils.locality_crosswalk import LocalityCrosswalk


def test_run_pipeline_writes_locality_crosswalk(monkeypatch, tmp_path) -> None:
    crosswalk = LocalityCrosswalk(
        [
            Locality(code="5000", name_he="תל אביב - יפו", district_he="תל אביב"),
            Locality(code="4000", name_he="חיפה", district_he="חיפה"),
        ]
    )
    monkeypatch.setattr("rent_collector.pipeline.get_crosswalk", lambda: crosswalk)
    output_path = tmp_path / "locality_crosswalk.csv"

    df = run_pipeline(validate=True, output_path=output_path)

    assert list(df["locality_code"]) == ["4000", "5000"]
    assert output_path.exists()


def test_run_pipeline_writes_without_validation(monkeypatch, tmp_path) -> None:
    crosswalk = LocalityCrosswalk(
        [Locality(code="5000", name_he="תל אביב - יפו", district_he="תל אביב")]
    )
    monkeypatch.setattr("rent_collector.pipeline.get_crosswalk", lambda: crosswalk)
    output_path = tmp_path / "locality_crosswalk.csv"

    df = run_pipeline(output_path=output_path)

    assert list(df["locality_code"]) == ["5000"]
    assert output_path.exists()


def test_run_pipeline_dry_run_returns_empty(monkeypatch) -> None:
    monkeypatch.setattr(
        "rent_collector.pipeline.probe_all",
        lambda *_args, **_kwargs: {"data-gov-il": {"ok": True}},
    )
    result = run_pipeline(dry_run=True)
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_validate_crosswalk_rejects_missing_columns_and_duplicates() -> None:
    with pytest.raises(ValidationFailedError, match="missing required columns"):
        _validate_crosswalk(pd.DataFrame([{"locality_code": "5000"}]))

    with pytest.raises(ValidationFailedError, match="duplicate locality_code"):
        _validate_crosswalk(
            pd.DataFrame(
                [
                    {
                        "locality_code": "5000",
                        "locality_name_he": "תל אביב - יפו",
                        "locality_name_en": "",
                        "district_he": "תל אביב",
                        "district_en": "Tel Aviv",
                        "population_approx": None,
                        "source": "data.gov.il",
                    },
                    {
                        "locality_code": "5000",
                        "locality_name_he": "תל אביב - יפו",
                        "locality_name_en": "",
                        "district_he": "תל אביב",
                        "district_en": "Tel Aviv",
                        "population_approx": None,
                        "source": "data.gov.il",
                    },
                ]
            )
        )


def test_probe_all_uses_only_data_gov_il(monkeypatch) -> None:
    monkeypatch.setattr(
        "rent_collector.pipeline.DataGovILCollector.probe",
        lambda self: {"ok": True, "sample_keys": ["סמל_ישוב"]},
    )
    results = probe_all()
    assert results == {"data-gov-il": {"ok": True, "sample_keys": ["סמל_ישוב"]}}


def test_crosswalk_dataframe_preserves_expected_columns() -> None:
    crosswalk = LocalityCrosswalk(
        [Locality(code="5000", name_he="תל אביב - יפו", district_he="תל אביב")]
    )
    df = _crosswalk_dataframe(crosswalk)
    assert list(df.columns) == [
        "locality_code",
        "locality_name_he",
        "locality_name_en",
        "district_he",
        "district_en",
        "population_approx",
        "source",
    ]


def test_crosswalk_dataframe_sorts_non_numeric_codes_without_crashing() -> None:
    crosswalk = LocalityCrosswalk(
        [
            Locality(code="5000", name_he="תל אביב - יפו", district_he="תל אביב"),
            Locality(code="ABC", name_he="בדיקה", district_he="מרכז"),
        ]
    )

    df = _crosswalk_dataframe(crosswalk)

    assert list(df["locality_code"]) == ["5000", "ABC"]


def test_normalize_sources_handles_none_all_valid_mixed_and_unknown() -> None:
    assert _normalize_sources(None) == ["data-gov-il"]
    assert _normalize_sources(["all"]) == ["data-gov-il"]
    assert _normalize_sources(["data-gov-il"]) == ["data-gov-il"]
    assert _normalize_sources(["data-gov-il", "unknown"]) == ["data-gov-il"]

    with pytest.raises(click.UsageError, match="No valid sources selected"):
        _normalize_sources(["unknown"])


def test_validate_crosswalk_rejects_empty_blank_and_non_numeric_codes() -> None:
    with pytest.raises(ValidationFailedError, match="Crosswalk is empty"):
        _validate_crosswalk(
            pd.DataFrame(
                columns=[
                    "locality_code",
                    "locality_name_he",
                    "locality_name_en",
                    "district_he",
                    "district_en",
                    "population_approx",
                    "source",
                ]
            )
        )

    with pytest.raises(ValidationFailedError, match="non-numeric locality_code"):
        _validate_crosswalk(
            pd.DataFrame(
                [
                    {
                        "locality_code": "ABC",
                        "locality_name_he": "תל אביב - יפו",
                        "locality_name_en": "",
                        "district_he": "תל אביב",
                        "district_en": "Tel Aviv",
                        "population_approx": None,
                        "source": "data.gov.il",
                    }
                ]
            )
        )

    with pytest.raises(ValidationFailedError, match="blank locality_code"):
        _validate_crosswalk(
            pd.DataFrame(
                [
                    {
                        "locality_code": None,
                        "locality_name_he": "תל אביב - יפו",
                        "locality_name_en": "",
                        "district_he": "תל אביב",
                        "district_en": "Tel Aviv",
                        "population_approx": None,
                        "source": "data.gov.il",
                    }
                ]
            )
        )
