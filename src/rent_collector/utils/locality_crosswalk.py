"""
Locality crosswalk: CBS locality code ↔ Hebrew/English city name.

Priority of data sources (highest first):
  1. Live CBS registry fetched from data.gov.il  (full ~1,200 localities)
  2. Seed CSV bundled in the repo               (top ~50 cities; offline fallback)

Usage:
    from rent_collector.utils.locality_crosswalk import LocalityCrosswalk
    cw = LocalityCrosswalk.load()
    locality = cw.by_code("5000")    # → Locality(name_he="תל אביב-יפו", ...)
    locality = cw.by_name("חיפה")    # → Locality(code="4000", ...)
    all_codes = cw.all_codes()       # → ["3000", "4000", ...]
"""

from __future__ import annotations

import csv
import logging
from functools import lru_cache

from rich.console import Console

from rent_collector.config import (
    DATAGOV_API_BASE,
    LOCALITY_REGISTRY_RESOURCE_ID,
    SEED_LOCALITIES_CSV,
)
from rent_collector.models import Locality

logger = logging.getLogger(__name__)
console = Console(stderr=True)


class LocalityCrosswalk:
    """In-memory locality code ↔ name mapping."""

    def __init__(self, localities: list[Locality]) -> None:
        self._by_code: dict[str, Locality] = {loc.code: loc for loc in localities}
        # Also index by normalised locality name.
        self._by_name_he: dict[str, Locality] = {
            _normalize_name(loc.name_he): loc for loc in localities
        }
        self._by_name_en: dict[str, Locality] = {
            _normalize_name(loc.name_en): loc for loc in localities if loc.name_en
        }

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    def by_code(self, code: str) -> Locality | None:
        """Return locality by CBS numeric code (e.g. '5000')."""
        return self._by_code.get(str(code).strip().lstrip("0") or "0")

    def by_code_padded(self, code: str | int) -> Locality | None:
        """Accept both '5000' and 5000 and leading-zero variants."""
        try:
            c = str(int(code))  # normalise: '05000' → '5000'
        except (TypeError, ValueError):
            return None
        return self._by_code.get(c)

    def by_name(self, name_he: str) -> Locality | None:
        """Fuzzy-ish lookup by locality name after normalising case/spacing."""
        return self._by_name_he.get(_normalize_name(name_he))

    def by_name_en(self, name_en: str) -> Locality | None:
        """Lookup by English locality name after normalising case/spacing."""
        return self._by_name_en.get(_normalize_name(name_en))

    def all_codes(self) -> list[str]:
        return list(self._by_code.keys())

    def all_localities(self) -> list[Locality]:
        return list(self._by_code.values())

    def __len__(self) -> int:
        return len(self._by_code)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, *, force_seed: bool = False) -> LocalityCrosswalk:
        """
        Build the crosswalk.

        1. Try fetching the full registry from data.gov.il.
        2. If that fails (no network, timeout), fall back to the seed CSV.
        """
        if not force_seed:
            try:
                localities = _fetch_from_datagov()
                console.log(
                    f"[green]Locality registry loaded from data.gov.il "
                    f"({len(localities)} localities)[/green]"
                )
                return cls(localities)
            except Exception as exc:
                console.log(
                    f"[yellow]data.gov.il locality fetch failed ({exc}); "
                    f"falling back to seed CSV.[/yellow]"
                )

        localities = _load_seed_csv()
        console.log(f"[yellow]Using seed locality CSV ({len(localities)} entries).[/yellow]")
        return cls(localities)


def _fetch_from_datagov() -> list[Locality]:
    """
    Fetch all localities from the CBS registry published on data.gov.il.

    API endpoint:
        GET https://data.gov.il/api/3/action/datastore_search
            ?resource_id=5c78e9fa-c2e2-4771-93ff-7f400a12f7ba
            &limit=32000
    """
    from rent_collector.utils.http_client import get_client

    client = get_client()
    url = f"{DATAGOV_API_BASE}/datastore_search"
    data = client.get_json(
        url,
        params={
            "resource_id": LOCALITY_REGISTRY_RESOURCE_ID,
            "limit": 32000,
        },
    )

    if not data.get("success"):
        raise ValueError(f"data.gov.il API returned success=false: {data}")

    records = data["result"]["records"]
    localities: list[Locality] = []

    for rec in records:
        # Field names vary; try several known variants
        code = (
            rec.get("סמל_ישוב")
            or rec.get("settlement_code")
            or rec.get("SEMEL_YISHUV")
            or rec.get("yishuv_code")
            or ""
        )
        name_he = (
            rec.get("שם_ישוב")
            or rec.get("settlement_name")
            or rec.get("SHEM_YISHUV")
            or rec.get("yishuv_name")
            or ""
        )
        name_en = (
            rec.get("שם_ישוב_לועזי")
            or rec.get("SHEM_YISHUV_ENGLISH")
            or rec.get("yishuv_name_english")
            or ""
        )
        district_he = _district_name_he(rec)
        district_en = _district_name_en(district_he)
        sub_district_he = rec.get("שם_נפה") or rec.get("NAFA") or rec.get("לשכה") or ""
        pop = rec.get("סה_כ") or rec.get("total_population") or None

        if not code or not name_he:
            continue

        try:
            localities.append(
                Locality(
                    code=str(int(code)),
                    name_he=name_he.strip(),
                    name_en=name_en.strip(),
                    district_he=district_he.strip(),
                    district_en=district_en,
                    sub_district_he=sub_district_he.strip(),
                    population=int(pop) if pop else None,
                    source="data.gov.il",
                )
            )
        except (ValueError, TypeError):
            logger.debug("Skipping malformed locality record: %s", rec)

    return localities


def _load_seed_csv() -> list[Locality]:
    """Load the bundled seed CSV as a fallback."""
    localities: list[Locality] = []
    seed_path = SEED_LOCALITIES_CSV
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed CSV not found: {seed_path}")

    with open(seed_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                localities.append(
                    Locality(
                        code=str(int(row["locality_code"])),
                        name_he=row["locality_name_he"].strip(),
                        name_en=row.get("locality_name_en", "").strip(),
                        district_he=row.get("district_he", "").strip(),
                        district_en=_district_name_en(row.get("district_he", "").strip()),
                        population=int(row["population_approx"])
                        if row.get("population_approx")
                        else None,
                        source="seed_csv",
                    )
                )
            except (KeyError, ValueError) as exc:
                logger.debug("Skipping seed row %s: %s", row, exc)

    return localities


@lru_cache(maxsize=1)
def get_crosswalk() -> LocalityCrosswalk:
    """Cached singleton crosswalk for use throughout the pipeline."""
    return LocalityCrosswalk.load()


def _normalize_name(name: str) -> str:
    return " ".join(name.replace("-", " - ").split()).strip().lower()


def _district_name_he(record: dict[str, object]) -> str:
    explicit = str(
        record.get("שם_מחוז") or record.get("district_name") or record.get("MACHOZ") or ""
    ).strip()
    if explicit:
        return explicit

    raw_code = record.get("סמל_נפה") or record.get("NAFA_CODE")
    try:
        nafa_code = int(str(raw_code).strip())
    except (TypeError, ValueError):
        return ""

    district_prefix = int(str(nafa_code)[0])
    return {
        1: "ירושלים",
        2: "הצפון",
        3: "חיפה",
        4: "המרכז",
        5: "תל אביב",
        6: "הדרום",
        7: "יהודה ושומרון",
    }.get(district_prefix, "")


def _district_name_en(district_he: str) -> str:
    return {
        "ירושלים": "Jerusalem",
        "הצפון": "North",
        "חיפה": "Haifa",
        "המרכז": "Center",
        "תל אביב": "Tel Aviv",
        "הדרום": "South",
        "יהודה ושומרון": "Judea and Samaria",
    }.get(district_he, "")
