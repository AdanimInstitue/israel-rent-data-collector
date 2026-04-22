"""
Configuration for the public-safe reference-data collector.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _looks_like_repo_root(path: Path) -> bool:
    return (
        (path / "pyproject.toml").exists()
        and (path / "src" / "rent_collector").exists()
        and (path / "data" / "locality_codes_seed.csv").exists()
    )


def _detect_root_dir() -> Path:
    configured = os.getenv("RENT_COLLECTOR_ROOT_DIR")
    if configured:
        return Path(configured).expanduser().resolve()

    for candidate in [Path.cwd(), *Path.cwd().parents]:
        if _looks_like_repo_root(candidate):
            return candidate.resolve()

    return Path(__file__).resolve().parents[2]


ROOT_DIR = _detect_root_dir()
DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"
RUN_ARTIFACTS_DIR = ROOT_DIR / "var" / "runs"

SEED_LOCALITIES_CSV = DATA_DIR / "locality_codes_seed.csv"

REQUEST_DELAY_SECONDS: float = float(os.getenv("REQUEST_DELAY_SECONDS", "1.2"))
REQUEST_TIMEOUT_SECONDS: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "30.0"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "4"))

USER_AGENT: str = (
    "AdanimInstituteResearch/0.1 "
    "(public-reference-data; contact: research@adanim.org.il) "
    "python-requests"
)

DATAGOV_API_BASE: str = "https://data.gov.il/api/3/action"
LOCALITY_REGISTRY_RESOURCE_ID: str = "5c78e9fa-c2e2-4771-93ff-7f400a12f7ba"

LOCALITY_CROSSWALK_CSV: Path = OUTPUT_DIR / "locality_crosswalk.csv"
