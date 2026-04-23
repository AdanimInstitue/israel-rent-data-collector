from __future__ import annotations

import json
import runpy
import subprocess
from pathlib import Path

import click
import pandas as pd
from click.testing import CliRunner

from rent_collector import __version__
from rent_collector.cli import _git_sha, _update_latest_pointers, main
from rent_collector.config import LOCALITY_CROSSWALK_CSV


def test_probe_command_returns_success(monkeypatch) -> None:
    monkeypatch.setattr(
        "rent_collector.pipeline.probe_all",
        lambda *_args, **_kwargs: {"data-gov-il": {"ok": True}},
    )
    result = CliRunner().invoke(main, ["--probe"])
    assert result.exit_code == 0
    assert "1/1 sources reachable." in result.output


def test_dry_run_empty_does_not_exit_nonzero(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("rent_collector.pipeline.run_pipeline", lambda **_: pd.DataFrame())
    result = CliRunner().invoke(main, ["--dry-run", "--output", str(tmp_path / "out.csv")])
    assert result.exit_code == 0


def test_source_all_is_forwarded_verbatim_to_pipeline(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    def _run_pipeline(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame([{"locality_code": "5000"}])

    monkeypatch.setattr("rent_collector.pipeline.run_pipeline", _run_pipeline)
    result = CliRunner().invoke(main, ["--source", "all", "--output", str(tmp_path / "out.csv")])
    assert result.exit_code == 0
    assert captured["sources"] == ["all"]


def test_cli_writes_run_artifacts(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("RENT_COLLECTOR_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setattr(
        "rent_collector.pipeline.run_pipeline",
        lambda **_: pd.DataFrame([{"locality_code": "5000", "source": "data.gov.il"}]),
    )
    result = CliRunner().invoke(main, ["--output", str(tmp_path / "out.csv")])
    assert result.exit_code == 0
    latest = json.loads((tmp_path / "runs" / "latest.json").read_text(encoding="utf-8"))
    run_dir = Path(latest["latest_run_dir"])
    run_record = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    assert run_record["exit_code"] == 0
    assert run_record["status"] == "success"
    assert Path(run_record["stdout_log"]).exists()
    assert Path(run_record["stderr_log"]).exists()


def test_full_command_exits_nonzero_when_validation_fails(monkeypatch, tmp_path) -> None:
    from rent_collector.pipeline import ValidationFailedError

    monkeypatch.setattr(
        "rent_collector.pipeline.run_pipeline",
        lambda **_: (_ for _ in ()).throw(ValidationFailedError("crosswalk failed")),
    )
    result = CliRunner().invoke(main, ["--validate", "--output", str(tmp_path / "out.csv")])
    assert result.exit_code == 1
    assert "crosswalk failed" in result.output


def test_module_main_invokes_click_entrypoint(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["rent_collector.cli", "--help"])
    try:
        runpy.run_module("rent_collector.cli", run_name="__main__")
    except SystemExit as exc:
        assert exc.code == 0


def test_subcommands_reject_top_level_execution_flags() -> None:
    result = CliRunner().invoke(main, ["--source", "data-gov-il", "sources", "list"])
    assert result.exit_code != 0
    assert "--source" in result.output


def test_sources_list_command_prints_registry() -> None:
    result = CliRunner().invoke(main, ["sources", "list"])
    assert result.exit_code == 0
    assert "data_gov_il_locality_registry" in result.output
    assert "collector=data-gov-il" in result.output


def test_subcommands_reject_explicit_default_output_value() -> None:
    result = CliRunner().invoke(main, ["--output", str(LOCALITY_CROSSWALK_CSV), "sources", "list"])
    assert result.exit_code != 0
    assert "--output" in result.output


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
    monkeypatch.setattr("rent_collector.cli.PUBLIC_BUNDLE_DIR", tmp_path / "bundle")
    monkeypatch.setattr(
        "rent_collector.cli.PUBLIC_LOCALITY_CROSSWALK_CSV",
        tmp_path / "bundle" / "locality_crosswalk.csv",
    )
    monkeypatch.setattr(
        "rent_collector.cli.PUBLIC_SOURCE_INVENTORY_CSV",
        tmp_path / "bundle" / "source_inventory.csv",
    )
    monkeypatch.setattr(
        "rent_collector.cli.PUBLIC_MANIFEST_JSON",
        tmp_path / "bundle" / "manifest.json",
    )
    monkeypatch.setattr(
        "rent_collector.cli.write_source_inventory_csv",
        lambda path: path.write_text("ok\n", encoding="utf-8"),
    )
    (tmp_path / "bundle").mkdir()
    (tmp_path / "bundle" / "locality_crosswalk.csv").write_text("col\n1\n", encoding="utf-8")

    captured: dict[str, object] = {}

    def _write_manifest(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"collector_version": kwargs["collector_version"]}

    monkeypatch.setattr("rent_collector.cli.write_manifest", _write_manifest)
    result = CliRunner().invoke(main, ["write-manifest"])
    assert result.exit_code == 0
    assert captured["collector_version"] == __version__
    assert captured["output_path"] == tmp_path / "bundle" / "manifest.json"
    assert captured["artifact_paths"] == [
        tmp_path / "bundle" / "locality_crosswalk.csv",
        tmp_path / "bundle" / "source_inventory.csv",
    ]


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


def test_full_command_records_click_exceptions(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("RENT_COLLECTOR_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setattr(
        "rent_collector.pipeline.run_pipeline",
        lambda **_: (_ for _ in ()).throw(click.ClickException("usage failed")),
    )

    result = CliRunner().invoke(main, ["--output", str(tmp_path / "out.csv")])

    assert result.exit_code == 1
    latest = json.loads((tmp_path / "runs" / "latest.json").read_text(encoding="utf-8"))
    run_dir = Path(latest["latest_run_dir"])
    run_record = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    assert run_record["status"] == "failure"
    assert run_record["error"] == "usage failed"


def test_csv_row_count_handles_missing_and_existing_files(tmp_path) -> None:
    from rent_collector.cli import _csv_row_count

    assert _csv_row_count(tmp_path / "missing.csv") == 0

    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("a\n1\n2\n", encoding="utf-8")
    assert _csv_row_count(csv_path) == 2


def test_git_sha_returns_none_when_git_command_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        "rent_collector.cli.subprocess.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(subprocess.CalledProcessError(1, "git")),
    )
    assert _git_sha() is None


def test_update_latest_pointers_ignores_symlink_failures(monkeypatch, tmp_path) -> None:
    run_dir = tmp_path / "run-001"
    run_dir.mkdir()
    monkeypatch.setattr(
        Path,
        "symlink_to",
        lambda self, *args, **kwargs: (_ for _ in ()).throw(OSError("no symlink")),
    )

    _update_latest_pointers(tmp_path, run_dir)

    latest = json.loads((tmp_path / "latest.json").read_text(encoding="utf-8"))
    assert latest["latest_run_dir"] == str(run_dir)


def test_subcommands_reject_all_top_level_execution_flags(tmp_path) -> None:
    result = CliRunner().invoke(
        main,
        [
            "--dry-run",
            "--probe",
            "--validate",
            "--run-dir",
            str(tmp_path / "runs"),
            "--verbose",
            "sources",
            "list",
        ],
    )
    assert result.exit_code != 0
    for flag in ["--dry-run", "--probe", "--validate", "--run-dir", "--verbose"]:
        assert flag in result.output


def test_cli_uses_explicit_run_dir(monkeypatch, tmp_path) -> None:
    run_dir = tmp_path / "custom-run-dir"
    monkeypatch.setattr(
        "rent_collector.pipeline.run_pipeline",
        lambda **_: pd.DataFrame([{"locality_code": "5000", "source": "data.gov.il"}]),
    )

    result = CliRunner().invoke(
        main,
        ["--run-dir", str(run_dir), "--output", str(tmp_path / "out.csv")],
    )

    assert result.exit_code == 0
    assert run_dir.exists()
    assert (run_dir / "run.json").exists()
