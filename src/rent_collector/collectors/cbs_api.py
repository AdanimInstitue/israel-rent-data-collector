"""
CBS REST API collector.

Fetches rent-price series from the Central Bureau of Statistics API
at api.cbs.gov.il.

Documentation: https://www.cbs.gov.il/en/Pages/Api-interface.aspx

Key endpoints used:
  GET /Index/Catalog/Catalog?lang=en          — XML catalog of all series
  GET /index/data/price?id={id}&lang=en&format=json — single series
  GET /index/data/price_all?lang=en&chapter={ch}&format=json — all in chapter

The CBS API returns price indices and average prices. For our use case we need:
  - Average monthly rent (NIS) by city + room group
  - This corresponds to the data in CBS Table 4.9

The exact series ID for Table 4.9 (average rent by city + room group) is
discovered by scanning the catalog. Known related series IDs:
  150230 — Actual rental prices index (national index, not absolute prices)
  40010  — Average Housing Indices and Prices (parent group)

EXECUTOR NOTE:
  Run `python scripts/collect.py --source cbs-api --scan-catalog` to print
  all series containing "rent" or "שכר" in their name.
  Then add the confirmed series ID to CBS_RENT_SERIES in config.py.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Iterator

from rich.console import Console

from rent_collector.collectors.base import BaseCollector
from rent_collector.config import CBS_API_BASE_URL, CBS_RENT_SERIES
from rent_collector.models import DataSource, RentObservation, RoomGroup
from rent_collector.utils.http_client import get_client
from rent_collector.utils.locality_crosswalk import get_crosswalk

logger = logging.getLogger(__name__)
console = Console(stderr=True)


class CBSApiCollector(BaseCollector):
    """Fetch rent price data from the CBS REST API."""

    name = "cbs_api"

    def __init__(self, *, dry_run: bool = False, scan_catalog: bool = False) -> None:
        super().__init__(dry_run=dry_run)
        self._scan_catalog = scan_catalog

    # ------------------------------------------------------------------
    # Catalog scan
    # ------------------------------------------------------------------

    def scan_catalog(self) -> list[dict[str, str]]:
        """
        Fetch the CBS series catalog and return all rent-related series.

        Returns a list of {id, name_he, name_en, chapter} dicts.
        """
        client = get_client()
        url = f"{CBS_API_BASE_URL}/Index/Catalog/Catalog"
        try:
            resp = client.get(url, params={"lang": "en"})
        except Exception as exc:
            logger.error("CBS catalog fetch failed: %s", exc)
            return []

        try:
            catalog = resp.json()
        except ValueError:
            # Older docs describe XML, but the live endpoint currently returns JSON.
            try:
                root = ET.fromstring(resp.text)
            except ET.ParseError as exc:
                logger.error("CBS catalog parse failed: %s", exc)
                return []
            catalog = {"chapters": []}
            for series in root.iter():
                sid = series.get("id") or series.get("Id") or series.get("seriesId")
                name = series.get("name") or series.get("Name")
                chapter = series.get("chapter") or series.get("Chapter")
                if sid and name:
                    catalog["chapters"].append(
                        {"mainCode": sid, "chapterName": name, "chapterId": chapter}
                    )

        rent_keywords = {"rent", "rental", "housing", "שכר", "שכירות", "דירה"}
        matches: list[dict[str, str]] = []

        for chapter in catalog.get("chapters", []):
            chapter_id = str(chapter.get("chapterId") or "")
            chapter_name = str(chapter.get("chapterName") or "")
            main_code = str(chapter.get("mainCode") or "")
            label = f"{chapter_name} {main_code}".lower()
            if any(k in label for k in rent_keywords):
                matches.append({"id": main_code, "name": chapter_name, "chapter": chapter_id})

            if not chapter_id:
                continue
            try:
                chapter_resp = client.get(
                    f"{CBS_API_BASE_URL}/index/data/price_all",
                    params={"lang": "en", "chapter": chapter_id, "format": "json"},
                )
                root = ET.fromstring(chapter_resp.text)
            except Exception:
                continue

            for index in root.findall(".//index"):
                sid = str(index.attrib.get("code") or "")
                name = (index.findtext("index_name") or "").strip()
                if sid and any(k in name.lower() for k in rent_keywords):
                    matches.append({"id": sid, "name": name, "chapter": chapter_id})

        deduped = list({(m["id"], m["name"], m["chapter"]): m for m in matches}.values())
        console.log(f"[green]CBS catalog: found {len(deduped)} rent-related series[/green]")
        for m in deduped[:40]:
            console.print(f"  {m['id']:10s} {m['name'][:80]}")

        if not any("average monthly prices of rent" in m["name"].lower() for m in deduped):
            console.log(
                "[yellow]No Table 4.9-equivalent city/room rent series was found in "
                "the public `api.cbs.gov.il/index` catalog.[/yellow]"
            )

        return deduped

    # ------------------------------------------------------------------
    # Single series fetch
    # ------------------------------------------------------------------

    def fetch_series(self, series_id: str) -> list[dict]:
        """
        Fetch a single CBS price series as a list of period-value dicts.

        Returns: [{period: "2024-Q4", value: 4200.0, ...}, ...]
        """
        client = get_client()
        url = f"{CBS_API_BASE_URL}/index/data/price"
        resp = client.get_json(
            url,
            params={
                "id": series_id,
                "lang": "en",
                "format": "json",
            },
        )
        # CBS API returns various structures depending on series type.
        # Common shapes:
        #   {"data": [{"period": "2024-Q4", "value": 4200, "description": "..."}]}
        #   {"Data": [{"Period": ..., "Value": ...}]}
        return _normalise_cbs_series(resp)

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def collect(self) -> Iterator[RentObservation]:
        """
        Yield rent observations from known CBS series.

        For national/district series: maps district codes to locality codes
        of the district's major city (best available for non-big-city localities).

        For city-level series (if the series has city-level breakdown): yields
        directly.
        """
        if self._scan_catalog:
            self.scan_catalog()

        if self.dry_run:
            console.log("[dim][dry-run] CBS API collect skipped.[/dim]")
            return

        if not CBS_RENT_SERIES:
            console.log(
                "[yellow]CBS API series list is empty because no verified Table 4.9 "
                "series is exposed by the current public index API.[/yellow]"
            )
            return

        for series_id, series_name in CBS_RENT_SERIES.items():
            console.log(f"Fetching CBS series {series_id}: {series_name}")
            try:
                rows = self.fetch_series(series_id)
                yield from _parse_cbs_series(rows, series_id, series_name)
            except Exception as exc:
                logger.warning("CBS series %s failed: %s", series_id, exc)

    def probe(self) -> dict[str, object]:
        client = get_client()
        url = f"{CBS_API_BASE_URL}/index/data/price"
        try:
            resp = client.get(
                f"{CBS_API_BASE_URL}/Index/Catalog/Catalog",
                params={"lang": "en"},
                raise_for_status=False,
            )
            return {
                "ok": resp.status_code == 200,
                "status_code": resp.status_code,
                "sample": resp.text[:200],
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _normalise_cbs_series(raw: object) -> list[dict]:
    """Normalise the various CBS API response shapes to a flat list of dicts."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "Data", "result", "Result", "items", "Items", "series"):
            inner = raw.get(key)
            if isinstance(inner, list):
                return inner
    logger.debug("CBS series: unrecognised shape %s", type(raw))
    return []


def _parse_cbs_series(
    rows: list[dict], series_id: str, series_name: str
) -> Iterator[RentObservation]:
    """
    Parse a CBS series into RentObservation instances.

    CBS series for Table 4.9 typically include:
      - period (e.g. "2024-Q4" or "2024-03")
      - locality / city code or name
      - room group
      - value (NIS)

    This function handles the most common shapes. If the series is a national
    index (no city breakdown), it yields one observation for each period
    with locality_code="NATIONAL".

    EXECUTOR NOTE: After fetching the actual series, inspect `rows[0]` to
    understand the field names and update this function.
    """
    crosswalk = get_crosswalk()

    for row in rows:
        # Try to extract period
        period_raw = (
            row.get("period") or row.get("Period")
            or row.get("date") or row.get("Date") or ""
        )
        year, quarter = _parse_period(str(period_raw))

        # Try to extract value
        value = None
        for vk in ("value", "Value", "price", "Price", "rent", "average", "Average"):
            v = row.get(vk)
            if v is not None:
                try:
                    value = float(v)
                    break
                except (TypeError, ValueError):
                    pass
        if value is None or value <= 0:
            continue

        # Try to extract room group
        room_raw = (
            row.get("rooms") or row.get("Rooms") or row.get("roomGroup")
            or row.get("numRooms") or row.get("description") or ""
        )
        room_group = _extract_room_group_from_label(str(room_raw))

        # Try to extract locality
        code_raw = (
            row.get("localityCode") or row.get("settlementCode")
            or row.get("cityCode") or row.get("areaCode") or ""
        )
        name_raw = (
            row.get("locality") or row.get("city") or row.get("area")
            or row.get("description") or ""
        )

        if code_raw:
            locality = crosswalk.by_code(str(code_raw))
        elif name_raw:
            locality = crosswalk.by_name(str(name_raw))
        else:
            locality = None

        locality_code = locality.code if locality else "NATIONAL"
        locality_name_he = locality.name_he if locality else str(name_raw) or "ארצי"
        locality_name_en = locality.name_en if locality else "National"

        if room_group is None:
            # Series without room breakdown: yield one entry per period
            yield RentObservation(
                locality_code=locality_code,
                locality_name_he=locality_name_he,
                locality_name_en=locality_name_en,
                room_group=RoomGroup.R3_0,  # placeholder; flag it
                avg_rent_nis=value,
                rent_nis=value,
                source=DataSource.CBS_API,
                year=year,
                quarter=quarter,
                notes=f"CBS series {series_id}; room group not in data",
            )
        else:
            yield RentObservation(
                locality_code=locality_code,
                locality_name_he=locality_name_he,
                locality_name_en=locality_name_en,
                room_group=room_group,
                avg_rent_nis=value,
                rent_nis=value,
                source=DataSource.CBS_API,
                year=year,
                quarter=quarter,
            )


def _parse_period(period: str) -> tuple[int, int]:
    """
    Parse a period string to (year, quarter).

    Handles: "2024-Q4", "2024-4", "2024-12", "2024", "Q4 2024", etc.
    """
    import re

    period = period.strip()
    # "2024-Q4" or "2024-4"
    m = re.search(r"(\d{4})[-/ ]?[Qq]?(\d)", period)
    if m:
        year = int(m.group(1))
        q_or_month = int(m.group(2))
        if q_or_month <= 4:
            return year, q_or_month
        # It's a month → convert to quarter
        return year, (q_or_month - 1) // 3 + 1
    # Just a year
    m2 = re.search(r"(\d{4})", period)
    if m2:
        return int(m2.group(1)), 4
    return 2025, 4


def _extract_room_group_from_label(label: str) -> RoomGroup | None:
    """Try to extract a RoomGroup from a label string."""
    import re

    label = label.lower().strip()
    # Look for a number (possibly with .5) in the label
    m = re.search(r"(\d+(?:\.\d)?)", label)
    if m:
        try:
            val = float(m.group(1))
            if 1 <= val <= 7:
                return RoomGroup.from_float(val)
        except ValueError:
            pass
    if "5+" in label or "5 ומעלה" in label or "five or more" in label:
        return RoomGroup.R5_PLUS
    return None
