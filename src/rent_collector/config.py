"""
Configuration for the rent collector.

All settings can be overridden via environment variables (see .env.example).
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[2]  # repo root
DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SEED_LOCALITIES_CSV = DATA_DIR / "locality_codes_seed.csv"

# ---------------------------------------------------------------------------
# HTTP behaviour
# ---------------------------------------------------------------------------

# Seconds between requests to the same host (polite rate-limiting)
REQUEST_DELAY_SECONDS: float = float(os.getenv("REQUEST_DELAY_SECONDS", "1.2"))

# Total request timeout in seconds
REQUEST_TIMEOUT_SECONDS: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "30.0"))

# Max retries on transient errors (5xx, connection reset, timeout)
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "4"))

# User-Agent string (identify ourselves as a research project)
USER_AGENT: str = (
    "AdanimInstituteResearch/0.1 "
    "(normative-rent-study; contact: research@adanim.org.il) "
    "python-httpx"
)

# ---------------------------------------------------------------------------
# Source: nadlan.gov.il
# ---------------------------------------------------------------------------

NADLAN_BASE_URL: str = "https://www.nadlan.gov.il"
NADLAN_DATA_BASE_URL: str = "https://data.nadlan.gov.il/api"

# The proxy backend used by nadlan.gov.il's SPA to serve JSON data.
# Per Gemini technical spec (docs/Israel_Housing_Data_Engineering_Advice_by_Gemini.md):
#   "nadlan.gov.il trend maps fetch JSON objects via XHR from proxy-nadlan.taxes.gov.il"
NADLAN_PROXY_BASE_URL: str = "https://proxy-nadlan.taxes.gov.il"

# Candidate API endpoint patterns — tried in order; first success wins.
# Pattern 0 uses the known proxy host; patterns 1-4 fall back to the main domain.
# Run `python scripts/collect.py --probe` to discover which is live.
NADLAN_RENT_ENDPOINTS: list[str] = [
    "/pages/settlement/rent/{id}.json",
    # Pattern 0: known proxy host (most likely to work — from Gemini analysis)
    # Full URL built in nadlan.py as: NADLAN_PROXY_BASE_URL + path
    "PROXY:/api/getRentsBySettlement",
    "PROXY:/api/settlement/{id}/rent",
    "PROXY:/RentBySettlement",
    # Patterns 1-4: fallback to main domain
    "/api/getRentsBySettlement",           # pattern 1
    "/api/settlement/{id}/rent",           # pattern 2 (RESTful)
    "/api/RentAnalysis/GetRentBySettlement",  # pattern 3 (action-based)
    "/NadlanAPI/GetRentBySettlementCode",  # pattern 4 (older style)
]

# Quarter to target (None = latest available)
NADLAN_TARGET_YEAR: int | None = int(os.getenv("NADLAN_TARGET_YEAR", "2025")) or None
NADLAN_TARGET_QUARTER: int | None = None  # 1-4 or None for latest

# ---------------------------------------------------------------------------
# Source: CBS REST API
# ---------------------------------------------------------------------------

CBS_API_BASE_URL: str = "https://api.cbs.gov.il"

# Known series IDs relevant to rent (verified from catalog; see sources.md)
# Run collectors/cbs_api.py with --scan-catalog to discover additional series.
# As of 2026-04-21, the public `api.cbs.gov.il/index` catalog does not expose
# a city-by-room average-rent series matching Table 4.9. Keep this empty until a
# confirmed series ID is found in a CBS API family that actually serves it.
CBS_RENT_SERIES: dict[str, str] = {}

# ---------------------------------------------------------------------------
# Source: CBS Table 4.9 (PDF / Excel)
# ---------------------------------------------------------------------------

# URL template for the Excel version of Table 4.9 from the monthly CPI release.
# Variables: {year} (4-digit), {month:02d} (01-12), {letter} ('a' for Jan etc.)
# Example: https://www.cbs.gov.il/he/publications/Madad/DocLib/2025/price09a/a4_9_e.xlsx
CBS_TABLE49_URL_TEMPLATE: str = (
    "https://www.cbs.gov.il/he/publications/Madad/DocLib"
    "/{year}/price{month:02d}{letter}/a4_9_e.xlsx"
)
CBS_TABLE49_PDF_URL_TEMPLATE: str = (
    "https://www.cbs.gov.il/he/publications/Madad/DocLib"
    "/{year}/price{month:02d}{letter}/a4_9_e.pdf"
)

# Most recent known publication (update when CBS releases a new one)
CBS_TABLE49_LATEST_YEAR: int = int(os.getenv("CBS_TABLE49_YEAR", "2025"))
CBS_TABLE49_LATEST_MONTH: int = int(os.getenv("CBS_TABLE49_MONTH", "9"))
CBS_TABLE49_LATEST_LETTER: str = os.getenv("CBS_TABLE49_LETTER", "a")

# ---------------------------------------------------------------------------
# Source: CBS Locality Registry (data.gov.il)
# ---------------------------------------------------------------------------

DATAGOV_API_BASE: str = "https://data.gov.il/api/3/action"

# Resource ID for the CBS locality registry on data.gov.il
# (list of all Israeli localities with CBS numeric codes)
LOCALITY_REGISTRY_RESOURCE_ID: str = "5c78e9fa-c2e2-4771-93ff-7f400a12f7ba"

# ---------------------------------------------------------------------------
# Source: Bank of Israel Hedonic Paper
# ---------------------------------------------------------------------------

BOI_HEDONIC_PAPER_URL: str = "https://www.boi.org.il/media/yulnw1sl/part-3n.pdf"

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

RENT_BENCHMARKS_CSV: Path = OUTPUT_DIR / "rent_benchmarks.csv"
LOCALITY_CROSSWALK_CSV: Path = OUTPUT_DIR / "locality_crosswalk.csv"
