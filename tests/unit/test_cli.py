from __future__ import annotations

import json
import runpy
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from rent_collector import __version__
from rent_collector.cli import main


def test_probe_command_returns_success(monkeypatch) -> None:
    monkeypatch.setattr(
        "rent_collector.pipeline.probe_all",
        lambda: {
            "nadlan": {"ok": True},
            "cbs-table49": {"ok": False},
        },
    )

    result = CliRunner().invoke(main, ["--probe"])

    assert result.exit_code == 0
    assert "1/2 sources reachable." in result.output


def test_full_command_exits_nonzero_when_pipeline_returns_empty(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("rent_collector.pipeline.run_pipeline", lambda **_: pd.DataFrame())

    result = CliRunner().invoke(main, ["--output", str(tmp_path / "out.csv")])

    assert result.exit_code == 1
    assert "No data collected." in result.output


def test_dry_run_empty_does_not_exit_nonzero(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("rent_collector.pipeline.run_pipeline", lambda **_: pd.DataFrame())

    result = CliRunner().invoke(main, ["--dry-run", "--output", str(tmp_path / "out.csv")])

    assert result.exit_code == 0


def test_source_all_maps_to_default_all_sources(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    def _run_pipeline(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame([{"rent_nis": 1}])

    monkeypatch.setattr("rent_collector.pipeline.run_pipeline", _run_pipeline)

    result = CliRunner().invoke(main, ["--source", "all", "--output", str(tmp_path / "out.csv")])

    assert result.exit_code == 0
    assert captured["sources"] is None


def test_cli_writes_run_artifacts(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("RENT_COLLECTOR_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setattr(
        "rent_collector.pipeline.run_pipeline",
        lambda **_: pd.DataFrame([{"rent_nis": 1, "source": "nadlan.gov.il"}]),
    )

    result = CliRunner().invoke(main, ["--output", str(tmp_path / "out.csv")])

    assert result.exit_code == 0

    latest = json.loads((tmp_path / "runs" / "latest.json").read_text(encoding="utf-8"))
    run_dir = Path(latest["latest_run_dir"])
    run_record = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))

    assert run_record["status"] == "success"
    assert run_record["exit_code"] == 0
    assert Path(run_record["stdout_log"]).exists()
    assert Path(run_record["stderr_log"]).exists()


def test_full_command_exits_nonzero_when_validation_fails(monkeypatch, tmp_path) -> None:
    from rent_collector.pipeline import ValidationFailedError

    monkeypatch.setattr(
        "rent_collector.pipeline.run_pipeline",
        lambda **_: (_ for _ in ()).throw(ValidationFailedError("rent bounds check failed")),
    )

    result = CliRunner().invoke(main, ["--validate", "--output", str(tmp_path / "out.csv")])

    assert result.exit_code == 1
    assert "rent bounds check failed" in result.output


def test_module_main_invokes_click_entrypoint(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["rent_collector.cli", "--help"])

    try:
        runpy.run_module("rent_collector.cli", run_name="__main__")
    except SystemExit as exc:
        assert exc.code == 0


@pytest.mark.parametrize(
    ("args", "flag"),
    [
        (["--dry-run", "sources", "list"], "--dry-run"),
        (["--probe", "sources", "list"], "--probe"),
        (["--scan-catalog", "sources", "list"], "--scan-catalog"),
        (["--source", "nadlan", "sources", "list"], "--source"),
        (["--validate", "sources", "list"], "--validate"),
        (["--expected-total-2022", "1", "sources", "list"], "--expected-total-2022"),
        (["--output", "custom.csv", "sources", "list"], "--output"),
        (["--run-dir", "runs/manual", "sources", "list"], "--run-dir"),
        (["--verbose", "sources", "list"], "--verbose"),
    ],
)
def test_subcommands_reject_top_level_execution_flags(args: list[str], flag: str) -> None:
    result = CliRunner().invoke(main, args)

    assert result.exit_code != 0
    assert flag in result.output


def test_sources_list_command_prints_registry() -> None:
    result = CliRunner().invoke(main, ["sources", "list"])

    assert result.exit_code == 0
    assert "nadlan_gov_il" in result.output


def test_build_public_bundle_subcommand_writes_manifest(monkeypatch) -> None:
    monkeypatch.setattr(
        "rent_collector.cli.build_public_bundle",
        lambda validate=True: {"collector_version": __version__, "validate": validate},
    )

    result = CliRunner().invoke(main, ["build-public-bundle"])

    assert result.exit_code == 0
    assert __version__ in result.output


def test_validate_public_bundle_subcommand_handles_success_and_failure(monkeypatch) -> None:
    monkeypatch.setattr("rent_collector.cli.validate_public_bundle", lambda: [])
    success = CliRunner().invoke(main, ["validate-public-bundle"])
    assert success.exit_code == 0
    assert "validation passed" in success.output.lower()

    monkeypatch.setattr("rent_collector.cli.validate_public_bundle", lambda: ["boom"])
    failure = CliRunner().invoke(main, ["validate-public-bundle"])
    assert failure.exit_code != 0
    assert "boom" in failure.output


def test_write_manifest_subcommand_uses_package_version(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("rent_collector.cli.ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        "rent_collector.cli.write_source_inventory_csv",
        lambda path: path.write_text("ok\n", encoding="utf-8"),
    )

    captured: dict[str, object] = {}

    def _write_manifest(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"collector_version": kwargs["collector_version"]}

    monkeypatch.setattr("rent_collector.cli.write_manifest", _write_manifest)

    result = CliRunner().invoke(main, ["write-manifest"])

    assert result.exit_code == 0
    assert captured["collector_version"] == __version__


def test_full_command_records_unexpected_exceptions(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("RENT_COLLECTOR_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setattr(
        "rent_collector.pipeline.run_pipeline",
        lambda **_: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = CliRunner().invoke(main, ["--output", str(tmp_path / "out.csv")])

    assert result.exit_code == 1
    latest = json.loads((tmp_path / "runs" / "latest.json").read_text(encoding="utf-8"))
    run_dir = Path(latest["latest_run_dir"])
    run_record = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    assert run_record["status"] == "failure"
    assert run_record["error"] == "boom"
