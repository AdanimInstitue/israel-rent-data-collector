"""
Main collection pipeline.

Orchestrates all collectors in priority order, merges results, deduplicates
(preferring higher-quality sources), and writes the final output CSV.

Priority order (higher = preferred when same locality+room_group exists in
multiple sources):
  1. nadlan.gov.il    (median rent; most granular)
  2. CBS Table 4.9    (average rent; official cross-check)
  3. CBS API          (average rent; national indices)
  4. BoI hedonic      (modelled fallback)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

import pandas as pd
from rich.console import Console
from rich.table import Table

from rent_collector.collectors.base import BaseCollector
from rent_collector.collectors.boi_hedonic import BoIHedonicCollector
from rent_collector.collectors.cbs_api import CBSApiCollector
from rent_collector.collectors.cbs_table49 import CBSTable49Collector
from rent_collector.collectors.data_gov_il import DataGovILCollector
from rent_collector.collectors.nadlan import NadlanCollector
from rent_collector.config import LOCALITY_CROSSWALK_CSV, RENT_BENCHMARKS_CSV
from rent_collector.models import DataSource, RentObservation
from rent_collector.utils.locality_crosswalk import LocalityCrosswalk, get_crosswalk

logger = logging.getLogger(__name__)
console = Console()

# Source priority for deduplication (lower index = higher priority)
SOURCE_PRIORITY: list[DataSource] = [
    DataSource.NADLAN,
    DataSource.CBS_TABLE49,
    DataSource.CBS_API,
    DataSource.BOI_HEDONIC,
    DataSource.SEED,
]


def run_pipeline(
    *,
    sources: list[str] | None = None,
    dry_run: bool = False,
    validate: bool = False,
    expected_total_2022: float | None = None,
    scan_catalog: bool = False,
    output_path: Path = RENT_BENCHMARKS_CSV,
    crosswalk_path: Path = LOCALITY_CROSSWALK_CSV,
) -> pd.DataFrame:
    """
    Run the full collection pipeline.

    Args:
        sources: Which collectors to run. None = all. Allowed values:
                 "nadlan", "cbs-api", "cbs-table49", "boi-hedonic", "data-gov-il"
        dry_run: Probe endpoints but don't save output.
        validate: After collection, check that the total normative rent
                  estimate is in the expected range.
        expected_total_2022: Baseline total annual rent (NIS) from 2022.
                             Used for validation.
        scan_catalog: Pass to CBSApiCollector to print all CBS series.
    """
    all_sources = sources or ["nadlan", "cbs-table49", "cbs-api", "boi-hedonic"]
    console.rule("[bold blue]Israel Rent Data Collector[/bold blue]")

    # ------------------------------------------------------------------
    # 1. Build locality crosswalk
    # ------------------------------------------------------------------
    console.log("Building locality crosswalk…")
    crosswalk = get_crosswalk()
    console.log(f"  {len(crosswalk)} localities loaded.")

    # Save crosswalk CSV
    if not dry_run:
        _save_crosswalk(crosswalk, crosswalk_path)

    # ------------------------------------------------------------------
    # 2. Run collectors
    # ------------------------------------------------------------------
    all_observations: list[RentObservation] = []

    collector_map: dict[str, BaseCollector] = {
        "nadlan": NadlanCollector(dry_run=dry_run),
        "cbs-table49": CBSTable49Collector(dry_run=dry_run),
        "cbs-api": CBSApiCollector(dry_run=dry_run, scan_catalog=scan_catalog),
        "boi-hedonic": BoIHedonicCollector(dry_run=dry_run),
        "data-gov-il": DataGovILCollector(dry_run=dry_run),
    }

    for source_name in all_sources:
        collector = collector_map.get(source_name)
        if collector is None:
            console.log(f"[yellow]Unknown source: {source_name!r}. Skipping.[/yellow]")
            continue

        console.rule(f"[dim]{source_name}[/dim]")
        try:
            obs_list = list(collector.collect())
            all_observations.extend(obs_list)
            console.log(
                f"  [green]{source_name}:[/green] {len(obs_list):,} observations"
            )
        except Exception as exc:
            logger.error("%s collector failed: %s", source_name, exc, exc_info=True)
            console.log(f"  [red]{source_name} FAILED: {exc}[/red]")

    if not all_observations:
        console.log("[red]No observations collected. Check connectivity and endpoint configuration.[/red]")
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # 3. Merge and deduplicate
    # ------------------------------------------------------------------
    console.rule("Merging")
    df = _merge_observations(all_observations)
    console.log(f"  After merge: {len(df):,} (locality, room_group) pairs")

    # ------------------------------------------------------------------
    # 4. Validation
    # ------------------------------------------------------------------
    if validate:
        _validate(df, expected_total_2022)

    # ------------------------------------------------------------------
    # 5. Save
    # ------------------------------------------------------------------
    if not dry_run:
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        console.log(f"[bold green]Output saved to {output_path}[/bold green]")
    else:
        console.log(f"[dim][dry-run] Would save {len(df):,} rows to {output_path}[/dim]")

    _print_summary(df)
    return df


# ---------------------------------------------------------------------------
# Probe all endpoints
# ---------------------------------------------------------------------------


def probe_all() -> dict[str, dict]:
    """Probe all sources and return a status dict."""
    results: dict[str, dict] = {}
    collectors: list[tuple[str, BaseCollector]] = [
        ("nadlan", NadlanCollector()),
        ("cbs-api", CBSApiCollector()),
        ("cbs-table49", CBSTable49Collector()),
        ("boi-hedonic", BoIHedonicCollector()),
        ("data-gov-il", DataGovILCollector()),
    ]
    for name, collector in collectors:
        console.log(f"Probing {name}…")
        results[name] = collector.probe()
        status = "[green]OK[/green]" if results[name].get("ok") else "[red]FAIL[/red]"
        console.log(f"  {name}: {status} — {results[name]}")
    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _merge_observations(observations: list[RentObservation]) -> pd.DataFrame:
    """
    Convert observations to a DataFrame, deduplicate by (locality_code, room_group),
    preferring higher-priority sources.
    """
    rows = [obs.model_dump() for obs in observations]
    df = pd.DataFrame(rows)

    # Add priority column for sorting (lower = better)
    priority_map = {src: i for i, src in enumerate(SOURCE_PRIORITY)}
    df["_priority"] = df["source"].map(
        lambda s: priority_map.get(DataSource(s), 99)
    )

    # Sort by priority so best rows come first
    df = df.sort_values("_priority")

    # Deduplicate: keep the best row per (locality_code, room_group)
    df = df.drop_duplicates(subset=["locality_code", "room_group"], keep="first")
    df = df.drop(columns=["_priority"])
    df = df.sort_values(["locality_code", "room_group"]).reset_index(drop=True)

    return df


def _save_crosswalk(crosswalk: LocalityCrosswalk, path: Path) -> None:
    rows = [
        {
            "locality_code": loc.code,
            "locality_name_he": loc.name_he,
            "locality_name_en": loc.name_en,
            "district_he": loc.district_he,
            "sub_district_he": loc.sub_district_he,
            "population": loc.population,
        }
        for loc in crosswalk.all_localities()
    ]
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    console.log(f"  Crosswalk saved to {path}")


def _validate(df: pd.DataFrame, expected_total_2022: float | None) -> None:
    """
    Validation: sum of all normative rents should be ≥ expected_total_2022.

    This mirrors the check discussed in the Shay-Nethanel kickoff:
    the 2022 reported total was ~131M NIS/year; our 2025 estimate should
    be at least that.
    """
    console.rule("Validation")

    if df.empty:
        console.log("[red]Empty DataFrame — nothing to validate.[/red]")
        return

    total_monthly = df["rent_nis"].sum()
    total_annual = total_monthly * 12
    console.log(f"  Sum of all rent_nis (monthly): {total_monthly:,.0f} NIS")
    console.log(f"  Annualised: {total_annual:,.0f} NIS")

    if expected_total_2022:
        if total_annual >= expected_total_2022:
            console.log(
                f"  [green]✓ Annual total ({total_annual:,.0f}) ≥ "
                f"2022 baseline ({expected_total_2022:,.0f})[/green]"
            )
        else:
            console.log(
                f"  [red]✗ Annual total ({total_annual:,.0f}) < "
                f"2022 baseline ({expected_total_2022:,.0f}). "
                f"Check coverage and prices.[/red]"
            )

    # Coverage
    cities_covered = df["locality_code"].nunique()
    source_counts = df["source"].value_counts().to_dict()
    console.log(f"  Localities covered: {cities_covered}")
    console.log(f"  Rows by source: {source_counts}")


def _print_summary(df: pd.DataFrame) -> None:
    """Print a rich table summary of the output."""
    if df.empty:
        return

    console.rule("Summary")
    t = Table(title="Rent benchmarks by source")
    t.add_column("Source")
    t.add_column("Localities", justify="right")
    t.add_column("Rows", justify="right")
    t.add_column("Median rent (NIS)", justify="right")

    for src, grp in df.groupby("source"):
        t.add_row(
            str(src),
            str(grp["locality_code"].nunique()),
            str(len(grp)),
            f"{grp['rent_nis'].median():,.0f}",
        )
    console.print(t)

    # Room group breakdown
    room_table = Table(title="Median rent by room group (all localities)")
    room_table.add_column("Room group")
    room_table.add_column("Median rent (NIS)", justify="right")
    room_table.add_column("Count", justify="right")

    for rg, grp in df.groupby("room_group"):
        room_table.add_row(str(rg), f"{grp['rent_nis'].median():,.0f}", str(len(grp)))
    console.print(room_table)
