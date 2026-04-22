from __future__ import annotations

import importlib
from pathlib import Path

from rent_collector import config as config_mod
from rent_collector.collectors.base import BaseCollector
from rent_collector.models import Locality


class _Collector(BaseCollector):
    def __init__(self, items: list[object] | Exception | None) -> None:
        super().__init__()
        self._items = items

    def collect(self):
        if isinstance(self._items, Exception):
            raise self._items
        return iter(self._items or [])


def test_locality_model_and_base_probe() -> None:
    locality = Locality(code="5000", name_he="תל אביב - יפו", district_he="תל אביב")
    result = _Collector([locality]).probe()
    assert result["ok"] is True
    assert result["sample"]["code"] == "5000"


def test_base_probe_handles_empty_and_error() -> None:
    assert _Collector([]).probe()["note"] == "no data returned"
    assert _Collector(RuntimeError("boom")).probe() == {"ok": False, "error": "boom"}


def test_config_detects_repo_root_from_cwd(monkeypatch, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "src" / "rent_collector").mkdir(parents=True)
    (repo / "data").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[project]\nname='demo'\n", encoding="utf-8")
    (repo / "data" / "locality_codes_seed.csv").write_text(
        "locality_code,locality_name_he\n5000,תל אביב - יפו\n", encoding="utf-8"
    )

    monkeypatch.delenv("RENT_COLLECTOR_ROOT_DIR", raising=False)
    monkeypatch.chdir(repo / "src" / "rent_collector")

    reloaded = importlib.reload(config_mod)
    try:
        assert reloaded.ROOT_DIR == repo.resolve()
    finally:
        importlib.reload(config_mod)
