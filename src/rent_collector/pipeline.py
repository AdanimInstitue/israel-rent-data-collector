"""
Main collection pipeline.

Orchestrates all collectors in priority order, merges results, deduplicates
(preferring higher-quality sources), and writes the final output CSV.

Priority order (higher = preferred when same locality+room_group exists in
multiple sources):
  1. nadlan.gov.il    (most granular locality+room observations; live payload is
                       currently average-oriented rather than median-oriented)
  2. CBS Table 4.9    (average rent; official cross-check)
  3. CBS API          (average rent; national indices)
  4. BoI hedonic      (modelled fallback)
"""

from __future__ import annotations

import logging
from pathlib import Path

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

DEFAULT_SOURCES = ["nadlan", "cbs-table49", "cbs-api", "boi-hedonic", "data-gov-il"]

# Source priority for deduplication (lower index = higher priority)
SOURCE_PRIORITY: list[DataSource] = [
    DataSource.NADLAN,
    DataSource.CBS_TABLE49,
    DataSource.CBS_API,
    DataSource.BOI_HEDONIC,
    DataSource.SEED,
]


class ValidationFailedError(ValueError):
    """Raised when pipeline validation detects publish-blocking output issues."""


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
        dry_run: Probe each selected collector and skip writes.
        validate: After collection, check output shape and sanity bounds.
        expected_total_2022: Optional 2022 reference baseline (NIS), retained
                             for operator context only and not enforced.
        scan_catalog: Pass to CBSApiCollector to print all CBS series.
    """
    all_sources = sources or DEFAULT_SOURCES
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
        if dry_run:
            try:
                probe_result = collector.probe()
                status = "[green]OK[/green]" if probe_result.get("ok") else "[yellow]FAIL[/yellow]"
                console.log(f"  {status} probe: {probe_result}")
            except Exception as exc:
                logger.error("%s probe failed: %s", source_name, exc, exc_info=True)
                console.log(f"  [red]{source_name} probe FAILED: {exc}[/red]")
            continue
        try:
            obs_list = list(collector.collect())
            all_observations.extend(obs_list)
            console.log(f"  [green]{source_name}:[/green] {len(obs_list):,} observations")
        except Exception as exc:
            logger.error("%s collector failed: %s", source_name, exc, exc_info=True)
            console.log(f"  [red]{source_name} FAILED: {exc}[/red]")

    if dry_run:
        console.log("[dim][dry-run] Probe completed. No output files were written.[/dim]")
        return pd.DataFrame()

    if not all_observations:
        console.log(
            "[red]No observations collected. Check connectivity and endpoint configuration.[/red]"
        )
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
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        console.log(f"[bold green]Output saved to {output_path}[/bold green]")
    else:
        console.log(f"[dim][dry-run] Would save {len(df):,} rows to {output_path}[/dim]")

    _print_summary(df)
    return df


# ---------------------------------------------------------------------------
# Probe all endpoints
# ---------------------------------------------------------------------------


def probe_all() -> dict[str, dict[str, object]]:
    """Probe all sources and return a status dict."""
    results: dict[str, dict[str, object]] = {}
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
    rows = [obs.model_dump(mode="json") for obs in observations]
    df = pd.DataFrame(rows)

    # Add priority column for sorting (lower = better)
    priority_map = {src.value: i for i, src in enumerate(SOURCE_PRIORITY)}
    df["_priority"] = df["source"].map(lambda s: priority_map.get(str(s), 99))
    df["_sort_year"] = pd.to_numeric(df["year"], errors="coerce").fillna(-1).astype(int)
    df["_sort_quarter"] = pd.to_numeric(df["quarter"], errors="coerce").fillna(-1).astype(int)
    df["_sort_rent"] = pd.to_numeric(df["rent_nis"], errors="coerce").fillna(-1.0)

    # Sort by source priority first, then prefer the most recent within-source row.
    df = df.sort_values(
        by=[
            "_priority",
            "_sort_year",
            "_sort_quarter",
            "_sort_rent",
            "locality_code",
            "room_group",
        ],
        ascending=[True, False, False, False, True, True],
    )

    # Deduplicate: keep the best row per (locality_code, room_group)
    df = df.drop_duplicates(subset=["locality_code", "room_group"], keep="first")
    df = df.drop(columns=["_priority", "_sort_year", "_sort_quarter", "_sort_rent"])
    df = df.sort_values(["locality_code", "room_group"]).reset_index(drop=True)

    return df


def _save_crosswalk(crosswalk: LocalityCrosswalk, path: Path) -> None:
    rows = sorted(
        [
            {
                "locality_code": loc.code,
                "locality_name_he": loc.name_he,
                "locality_name_en": loc.name_en,
                "district_he": loc.district_he,
                "district_en": loc.district_en,
                "population_approx": loc.population,
                "source": loc.source,
            }
            for loc in crosswalk.all_localities()
        ],
        key=lambda row: (row["locality_code"], row["locality_name_he"]),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    console.log(f"  Crosswalk saved to {path}")


def _validate(df: pd.DataFrame, expected_total_2022: float | None) -> None:
    """
    Validate output shape and sanity bounds.

    The unweighted sum across locality × room rows is useful as a coarse trend
    indicator, but it is not directly comparable to the 2022 facility-level
    baseline used in the project handoff documents.
    """
    console.rule("Validation")

    if df.empty:
        raise ValidationFailedError("Empty DataFrame — nothing to validate.")

    if df["rent_nis"].isna().any():
        raise ValidationFailedError("Validation failed: output contains missing rent_nis values.")

    total_monthly = df["rent_nis"].sum()
    total_annual = total_monthly * 12
    console.log(f"  Sum of all rent_nis (monthly): {total_monthly:,.0f} NIS")
    console.log(f"  Annualised row-sum (informational only): {total_annual:,.0f} NIS")

    if expected_total_2022 is not None:
        console.log(
            "  [yellow]! The provided 2022 baseline is facility-level and is not directly "
            "comparable to this unweighted locality-by-room output.[/yellow]"
        )
        console.log(
            f"  2022 reference baseline retained for context: {expected_total_2022:,.0f} NIS"
        )

    min_rent = float(df["rent_nis"].min())
    max_rent = float(df["rent_nis"].max())
    if min_rent < 500 or max_rent > 20_000:
        message = (
            "Validation failed: rent bounds check failed "
            f"(min={min_rent:,.0f}, max={max_rent:,.0f})."
        )
        console.log(f"  [red]✗ {message}[/red]")
        raise ValidationFailedError(message)

    console.log(
        f"  [green]✓ Rent bounds check passed (min={min_rent:,.0f}, max={max_rent:,.0f}).[/green]"
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
