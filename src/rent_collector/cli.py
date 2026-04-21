"""
CLI entry point: `rent-collect` (or `python scripts/collect.py`).
"""

from __future__ import annotations

import logging
import sys

import click
from rich.console import Console
from rich.logging import RichHandler

from rent_collector.config import RENT_BENCHMARKS_CSV

console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )


@click.command()
@click.option(
    "--source",
    multiple=True,
    type=click.Choice(
        ["nadlan", "cbs-api", "cbs-table49", "boi-hedonic", "data-gov-il"],
        case_sensitive=False,
    ),
    default=[],
    help="Sources to collect from. Repeat to specify multiple. Default: all.",
)
@click.option("--dry-run", is_flag=True, help="Probe endpoints but don't save output.")
@click.option("--probe", is_flag=True, help="Probe all endpoints and exit.")
@click.option(
    "--scan-catalog",
    is_flag=True,
    help="(CBS API) Scan and print all rent-related series in the CBS catalog.",
)
@click.option(
    "--validate",
    is_flag=True,
    help="After collection, validate total against expected baseline.",
)
@click.option(
    "--expected-total-2022",
    type=float,
    default=None,
    help="Expected annual total normative rent from 2022 (NIS). Used with --validate.",
)
@click.option(
    "--output",
    type=click.Path(),
    default=str(RENT_BENCHMARKS_CSV),
    show_default=True,
    help="Output CSV path.",
)
@click.option("--verbose", "-v", is_flag=True, help="Debug logging.")
def main(
    source: tuple[str, ...],
    dry_run: bool,
    probe: bool,
    scan_catalog: bool,
    validate: bool,
    expected_total_2022: float | None,
    output: str,
    verbose: bool,
) -> None:
    """Collect official Israeli rental-price benchmarks from government sources."""
    _setup_logging(verbose)

    if probe:
        from rent_collector.pipeline import probe_all

        results = probe_all()
        ok_count = sum(1 for r in results.values() if r.get("ok"))
        console.print(f"\n{ok_count}/{len(results)} sources reachable.")
        sys.exit(0 if ok_count > 0 else 1)

    from pathlib import Path
    from rent_collector.pipeline import run_pipeline

    sources_list = list(source) if source else None  # None = all

    df = run_pipeline(
        sources=sources_list,
        dry_run=dry_run,
        validate=validate,
        expected_total_2022=expected_total_2022,
        scan_catalog=scan_catalog,
        output_path=Path(output),
    )

    if df.empty and not dry_run:
        console.print("[red]No data collected.[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
