"""
CLI entry point for the public-safe reference-data collector.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

import click
from rich.console import Console
from rich.logging import RichHandler

from rent_collector import __version__
from rent_collector.config import LOCALITY_CROSSWALK_CSV, ROOT_DIR, RUN_ARTIFACTS_DIR
from rent_collector.pipeline import ValidationFailedError
from rent_collector.provenance import write_manifest, write_source_inventory_csv
from rent_collector.public_bundle import (
    PUBLIC_BUNDLE_DIR,
    PUBLIC_LOCALITY_CROSSWALK_CSV,
    PUBLIC_MANIFEST_JSON,
    PUBLIC_SOURCE_INVENTORY_CSV,
    build_public_bundle,
    validate_public_bundle,
)
from rent_collector.source_registry import list_sources

console = Console()

_SOURCE_PIPELINE_KEYS = {
    "data_gov_il_locality_registry": "data-gov-il",
}


@dataclass
class _RunRecord:
    run_dir: Path
    started_at: datetime
    command: list[str]
    output_path: Path
    exit_code: int | None = None
    error: str | None = None


class _TeeStream:
    def __init__(self, primary: TextIO, mirror: TextIO) -> None:
        self._primary = primary
        self._mirror = mirror
        self.encoding = getattr(primary, "encoding", "utf-8")

    def write(self, data: str) -> int:
        written = self._primary.write(data)
        self._mirror.write(data)
        return written

    def flush(self) -> None:
        self._primary.flush()
        self._mirror.flush()

    def isatty(self) -> bool:
        return bool(getattr(self._primary, "isatty", lambda: False)())


def _git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT_DIR,
            capture_output=True,
            check=True,
            text=True,
        )
    except Exception:
        return None
    return result.stdout.strip() or None


def _default_runs_dir() -> Path:
    configured = os.getenv("RENT_COLLECTOR_RUNS_DIR")
    return Path(configured) if configured else RUN_ARTIFACTS_DIR


def _allocate_run_dir(base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    stem = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    candidate = base_dir / stem
    counter = 1
    while candidate.exists():
        counter += 1
        candidate = base_dir / f"{stem}-{counter:02d}"
    candidate.mkdir(parents=True, exist_ok=False)
    return candidate


def _update_latest_pointers(base_dir: Path, run_dir: Path) -> None:
    latest_json = base_dir / "latest.json"
    latest_payload = json.dumps({"latest_run_dir": str(run_dir)}, indent=2) + "\n"
    latest_json.write_text(latest_payload, encoding="utf-8")

    latest_link = base_dir / "latest"
    try:
        if latest_link.is_symlink() or latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(run_dir.name, target_is_directory=True)
    except OSError:
        pass


def _write_run_record(record: _RunRecord) -> None:
    finished_at = datetime.now(UTC)
    payload = {
        "run_dir": str(record.run_dir),
        "started_at": record.started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round((finished_at - record.started_at).total_seconds(), 3),
        "command": record.command,
        "cwd": str(ROOT_DIR),
        "git_sha": _git_sha(),
        "exit_code": record.exit_code,
        "status": "success" if record.exit_code == 0 else "failure",
        "error": record.error,
        "stdout_log": str(record.run_dir / "stdout.log"),
        "stderr_log": str(record.run_dir / "stderr.log"),
        "output_csv": str(record.output_path),
    }
    (record.run_dir / "run.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


@contextmanager
def _capture_run_streams(run_dir: Path) -> Iterator[None]:
    stdout_path = run_dir / "stdout.log"
    stderr_path = run_dir / "stderr.log"
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    with (
        stdout_path.open("w", encoding="utf-8") as stdout_log,
        stderr_path.open("w", encoding="utf-8") as stderr_log,
    ):
        sys.stdout = _TeeStream(original_stdout, stdout_log)
        sys.stderr = _TeeStream(original_stderr, stderr_log)
        try:
            yield
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            sys.stdout = original_stdout
            sys.stderr = original_stderr


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )


def _subcommand_conflicting_options(
    ctx: click.Context,
    *,
    source: tuple[str, ...],
    dry_run: bool,
    probe: bool,
    validate: bool,
    run_dir: Path | None,
    verbose: bool,
) -> list[str]:
    conflicts: list[str] = []
    if source:
        conflicts.append("--source")
    if dry_run:
        conflicts.append("--dry-run")
    if probe:
        conflicts.append("--probe")
    if validate:
        conflicts.append("--validate")
    if ctx.get_parameter_source("output") is click.core.ParameterSource.COMMANDLINE:
        conflicts.append("--output")
    if ctx.get_parameter_source("run_dir") is click.core.ParameterSource.COMMANDLINE:
        conflicts.append("--run-dir")
    if verbose:
        conflicts.append("--verbose")
    return conflicts


@click.group(invoke_without_command=True)
@click.option(
    "--source",
    multiple=True,
    type=click.Choice(["all", "data-gov-il"], case_sensitive=False),
    default=[],
    help="Sources to collect from. Repeat to specify multiple. Use 'all' or omit for all.",
)
@click.option("--dry-run", is_flag=True, help="Probe endpoints but do not save output.")
@click.option("--probe", is_flag=True, help="Probe all selected endpoints and exit.")
@click.option("--validate", is_flag=True, help="Validate the generated crosswalk.")
@click.option(
    "--output",
    type=click.Path(),
    default=str(LOCALITY_CROSSWALK_CSV),
    show_default=True,
    help="Output CSV path.",
)
@click.option(
    "--run-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    default=None,
    help="Optional run-artifact directory. Default: var/runs/<timestamp>/",
)
@click.option("--verbose", "-v", is_flag=True, help="Debug logging.")
@click.pass_context
def main(
    ctx: click.Context,
    source: tuple[str, ...],
    dry_run: bool,
    probe: bool,
    validate: bool,
    output: str,
    run_dir: Path | None,
    verbose: bool,
) -> None:
    """Collect and package public-safe Israeli locality reference data."""
    if ctx.invoked_subcommand is not None:
        conflicts = _subcommand_conflicting_options(
            ctx,
            source=source,
            dry_run=dry_run,
            probe=probe,
            validate=validate,
            run_dir=run_dir,
            verbose=verbose,
        )
        if conflicts:
            raise click.UsageError(
                "Top-level execution options cannot be combined with subcommands: "
                + ", ".join(conflicts)
            )
        return

    output_path = Path(output)
    actual_run_dir = run_dir or _allocate_run_dir(_default_runs_dir())
    if run_dir is not None:
        actual_run_dir.mkdir(parents=True, exist_ok=True)
    record = _RunRecord(
        run_dir=actual_run_dir,
        started_at=datetime.now(UTC),
        command=["indc", *sys.argv[1:]],
        output_path=output_path,
    )

    try:
        with _capture_run_streams(actual_run_dir):
            global console
            console = Console()
            console.print(f"[dim]Run artifacts: {actual_run_dir}[/dim]")
            _setup_logging(verbose)

            if probe:
                from rent_collector.pipeline import probe_all

                results = probe_all(list(source) if source else None)
                ok_count = sum(1 for result in results.values() if result.get("ok"))
                console.print(f"\n{ok_count}/{len(results)} sources reachable.")
                record.exit_code = 0 if ok_count > 0 else 1
                raise click.exceptions.Exit(record.exit_code)

            from rent_collector.pipeline import run_pipeline

            sources_list = list(source) if source else None

            try:
                run_pipeline(
                    sources=sources_list,
                    dry_run=dry_run,
                    validate=validate,
                    output_path=output_path,
                )
            except ValidationFailedError as exc:
                record.error = str(exc)
                record.exit_code = 1
                raise click.ClickException(str(exc)) from exc

            record.exit_code = 0
    except click.ClickException as exc:
        if record.error is None:
            record.error = exc.format_message()
        record.exit_code = record.exit_code if record.exit_code is not None else 1
        raise
    except click.exceptions.Exit as exc:
        record.exit_code = record.exit_code if record.exit_code is not None else exc.exit_code
        raise
    except Exception as exc:
        record.error = str(exc)
        record.exit_code = 1
        raise
    finally:
        _write_run_record(record)
        _update_latest_pointers(actual_run_dir.parent, actual_run_dir)


@main.group("sources")
def sources_group() -> None:
    """Inspect registered public-safe sources."""


@sources_group.command("list")
def list_sources_command() -> None:
    for source in list_sources():
        pipeline_key = _SOURCE_PIPELINE_KEYS.get(source.source_id, "n/a")
        console.print(
            f"{source.source_id}\tcollector={pipeline_key}\t{source.status}\t"
            f"{source.display_name}\t{source.homepage_url}"
        )


@main.command("build-public-bundle")
@click.option("--validate/--no-validate", "bundle_validate", default=True)
def build_public_bundle_command(bundle_validate: bool) -> None:
    manifest = build_public_bundle(validate=bundle_validate)
    console.print(f"Wrote public bundle manifest to {PUBLIC_MANIFEST_JSON}")
    console.print(json.dumps(manifest, indent=2, ensure_ascii=False))


@main.command("validate-public-bundle")
def validate_public_bundle_command() -> None:
    errors = validate_public_bundle()
    if errors:
        raise click.ClickException("\n".join(errors))
    console.print("Public bundle validation passed.")


@main.command("write-manifest")
def write_manifest_command() -> None:
    PUBLIC_BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
    write_source_inventory_csv(PUBLIC_SOURCE_INVENTORY_CSV)
    manifest = write_manifest(
        root_dir=ROOT_DIR,
        output_path=PUBLIC_MANIFEST_JSON,
        artifact_paths=[PUBLIC_LOCALITY_CROSSWALK_CSV, PUBLIC_SOURCE_INVENTORY_CSV],
        row_counts={
            "locality_crosswalk.csv": _csv_row_count(PUBLIC_LOCALITY_CROSSWALK_CSV),
            "source_inventory.csv": len(list_sources()),
        },
        collector_version=__version__,
    )
    console.print(json.dumps(manifest, indent=2, ensure_ascii=False))


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8", newline="") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


if __name__ == "__main__":
    main()
