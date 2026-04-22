"""
Public-safe reference-data pipeline.

This collector is intentionally limited to Category-1 public-safe reference
artifacts. Milestone 1 ships only the data.gov.il / CBS locality registry flow.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.table import Table

from rent_collector.collectors.data_gov_il import DataGovILCollector
from rent_collector.config import LOCALITY_CROSSWALK_CSV
from rent_collector.utils.locality_crosswalk import LocalityCrosswalk, get_crosswalk

logger = logging.getLogger(__name__)
console = Console()

DEFAULT_SOURCES = ["data-gov-il"]
REQUIRED_COLUMNS = [
    "locality_code",
    "locality_name_he",
    "locality_name_en",
    "district_he",
    "district_en",
    "population_approx",
    "source",
]


class ValidationFailedError(ValueError):
    """Raised when pipeline validation detects publish-blocking output issues."""


def run_pipeline(
    *,
    sources: list[str] | None = None,
    dry_run: bool = False,
    validate: bool = False,
    output_path: Path = LOCALITY_CROSSWALK_CSV,
) -> pd.DataFrame:
    """
    Build the public-safe locality crosswalk.

    Args:
        sources: Supported values are ``None``, ``all``, or ``data-gov-il``.
        dry_run: Probe sources and skip writes.
        validate: Validate the generated crosswalk before writing.
        output_path: Destination CSV path for the crosswalk.
    """
    selected_sources = _normalize_sources(sources)
    console.rule("[bold blue]Israel Reference Data Collector[/bold blue]")

    if dry_run:
        results = probe_all(selected_sources)
        ok_count = sum(1 for result in results.values() if result.get("ok"))
        console.log(f"[dim][dry-run][/dim] {ok_count}/{len(results)} sources reachable.")
        return pd.DataFrame()

    console.log("Building locality crosswalk from public-safe registry data…")
    crosswalk = get_crosswalk()
    df = _crosswalk_dataframe(crosswalk)
    console.log(f"  {len(df):,} localities loaded.")

    if validate:
        _validate_crosswalk(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    console.log(f"[bold green]Wrote locality crosswalk to {output_path}[/bold green]")
    _print_summary(df)
    return df


def probe_all(sources: list[str] | None = None) -> dict[str, dict[str, object]]:
    """Probe all selected public-safe sources and return a status map."""
    selected_sources = _normalize_sources(sources)
    collector_map = {"data-gov-il": DataGovILCollector()}
    results: dict[str, dict[str, object]] = {}
    for source_name in selected_sources:
        collector = collector_map[source_name]
        console.log(f"Probing {source_name}…")
        results[source_name] = collector.probe()
        status = "[green]OK[/green]" if results[source_name].get("ok") else "[red]FAIL[/red]"
        console.log(f"  {source_name}: {status} — {results[source_name]}")
    return results


def _normalize_sources(sources: list[str] | None) -> list[str]:
    if sources is None:
        return list(DEFAULT_SOURCES)

    normalized = [source.lower() for source in sources]
    if "all" in normalized:
        return list(DEFAULT_SOURCES)

    unknown_sources = [source for source in normalized if source not in DEFAULT_SOURCES]
    for source in unknown_sources:
        console.log(f"[yellow]Unknown source: {source!r}. Skipping.[/yellow]")
    selected = [source for source in normalized if source in DEFAULT_SOURCES]
    return selected or list(DEFAULT_SOURCES)


def _crosswalk_dataframe(crosswalk: LocalityCrosswalk) -> pd.DataFrame:
    rows = sorted(
        [
            {
                "locality_code": locality.code,
                "locality_name_he": locality.name_he,
                "locality_name_en": locality.name_en,
                "district_he": locality.district_he,
                "district_en": locality.district_en,
                "population_approx": locality.population,
                "source": locality.source,
            }
            for locality in crosswalk.all_localities()
        ],
        key=lambda row: (int(str(row["locality_code"])), str(row["locality_name_he"])),
    )
    return pd.DataFrame(rows, columns=REQUIRED_COLUMNS)


def _validate_crosswalk(df: pd.DataFrame) -> None:
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValidationFailedError(
            "Crosswalk is missing required columns: " + ", ".join(missing_columns)
        )

    if df.empty:
        raise ValidationFailedError("Crosswalk is empty.")

    if df["locality_code"].isna().any():
        raise ValidationFailedError("Crosswalk contains blank locality_code values.")

    if df["locality_code"].duplicated().any():
        duplicates = sorted(df.loc[df["locality_code"].duplicated(), "locality_code"].unique())
        raise ValidationFailedError(
            "Crosswalk contains duplicate locality_code values: " + ", ".join(duplicates[:10])
        )


def _print_summary(df: pd.DataFrame) -> None:
    table = Table(title="Reference Data Summary")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Localities", f"{len(df):,}")
    table.add_row("Districts", f"{df['district_en'].replace('', pd.NA).dropna().nunique():,}")
    table.add_row("Source families", f"{df['source'].nunique():,}")
    console.print(table)
