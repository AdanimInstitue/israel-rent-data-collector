"""
Collector for nadlan.gov.il rental-trend data.

nadlan.gov.il is the Israeli government real-estate portal. Its rental-trend
pages display median rent by locality + room group, backed by a JSON API that
was reverse-engineered by community projects (see sources.md).

Strategy:
  1. Try each endpoint pattern in NADLAN_RENT_ENDPOINTS in order.
  2. For each pattern, send a probe request against a known locality (Tel Aviv,
     code 5000) and inspect the response shape.
  3. Use the first pattern that returns valid rental data.
  4. If all API patterns fail (e.g. site restructured), fall back to HTML
     scraping of the public rental-trends pages.

The collector iterates over all localities in the crosswalk and fetches rent
for each one, yielding RentObservation instances.

To add a new endpoint pattern discovered by Codex / manual inspection, append
it to NADLAN_RENT_ENDPOINTS in config.py.

EXECUTOR NOTE (for Codex):
  Run `python scripts/collect.py --source nadlan --dry-run` to probe which
  endpoint is live.  Then inspect the JSON shape for that endpoint and update
  `_parse_response()` if needed.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

import requests
from rich.console import Console
from rich.progress import track

from rent_collector.collectors.base import BaseCollector
from rent_collector.config import (
    NADLAN_BASE_URL,
    NADLAN_DATA_BASE_URL,
    NADLAN_RENT_ENDPOINTS,
    NADLAN_TARGET_QUARTER,
    NADLAN_TARGET_YEAR,
)
from rent_collector.models import DataSource, RentObservation, RoomGroup
from rent_collector.utils.http_client import get_client
from rent_collector.utils.locality_crosswalk import get_crosswalk

logger = logging.getLogger(__name__)
console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Known response shapes for each endpoint pattern.
# These will be extended by the executor after live probing.
# ---------------------------------------------------------------------------

# Room-count label → RoomGroup mapping for nadlan.gov.il labels.
# The site uses Hebrew labels; extend this dict as you encounter new variants.
_ROOM_LABEL_MAP: dict[str, RoomGroup] = {
    "1": RoomGroup.R1_0,
    "1.5": RoomGroup.R1_5,
    "2": RoomGroup.R2_0,
    "2.5": RoomGroup.R2_5,
    "3": RoomGroup.R3_0,
    "3.5": RoomGroup.R3_5,
    "4": RoomGroup.R4_0,
    "4.5": RoomGroup.R4_5,
    "5": RoomGroup.R5_0,
    "5+": RoomGroup.R5_PLUS,
    "חדר 1": RoomGroup.R1_0,
    "חדר 1.5": RoomGroup.R1_5,
    "חדר 2": RoomGroup.R2_0,
    "2 חדרים": RoomGroup.R2_0,
    "2.5 חדרים": RoomGroup.R2_5,
    "3 חדרים": RoomGroup.R3_0,
    "3.5 חדרים": RoomGroup.R3_5,
    "4 חדרים": RoomGroup.R4_0,
    "4.5 חדרים": RoomGroup.R4_5,
    "5 חדרים": RoomGroup.R5_0,
    "5+ חדרים": RoomGroup.R5_PLUS,
    "5 ומעלה": RoomGroup.R5_PLUS,
}


class NadlanCollector(BaseCollector):
    """Fetch rental medians from nadlan.gov.il."""

    name = "nadlan"

    def __init__(
        self,
        *,
        dry_run: bool = False,
        locality_codes: list[str] | None = None,
    ) -> None:
        super().__init__(dry_run=dry_run)
        # If no locality codes provided, use all from the crosswalk
        self._locality_codes = locality_codes
        self._active_endpoint: str | None = None  # discovered during probe

    # ------------------------------------------------------------------
    # Endpoint discovery
    # ------------------------------------------------------------------

    def discover_endpoint(self) -> str | None:
        """
        Try each candidate endpoint pattern and return the first one that
        returns valid JSON data for Tel Aviv (code 5000).

        Returns the endpoint path (relative to NADLAN_BASE_URL) or None if
        all patterns fail.
        """
        probe_code = "5000"  # Tel Aviv-Yafo
        client = get_client()

        for pattern in NADLAN_RENT_ENDPOINTS:
            url_path = pattern.replace("{id}", probe_code)
            base = _base_url_for_pattern(pattern)
            url = f"{base}{url_path.replace('PROXY:', '')}"
            try:
                resp = client.get(
                    url,
                    params=_build_params(probe_code, pattern),
                    raise_for_status=False,
                )
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        if _looks_like_rent_data(data):
                            console.log(
                                f"[green]nadlan endpoint found:[/green] {url_path}"
                            )
                            self._active_endpoint = pattern
                            return pattern
                        else:
                            console.log(
                                f"[yellow]Endpoint {url_path} returned 200 but "
                                f"unrecognised shape: {str(data)[:120]}[/yellow]"
                            )
                    except Exception as parse_exc:
                        console.log(
                            f"[yellow]Endpoint {url_path}: 200 but JSON parse "
                            f"failed: {parse_exc}[/yellow]"
                        )
                else:
                    console.log(
                        f"[dim]Endpoint {url_path}: HTTP {resp.status_code}[/dim]"
                    )
            except requests.RequestException as exc:
                console.log(f"[dim]Endpoint {url_path}: {exc}[/dim]")

        console.log(
            "[red]All nadlan API endpoints failed. Will try HTML fallback.[/red]"
        )
        return None

    # ------------------------------------------------------------------
    # Main collection
    # ------------------------------------------------------------------

    def collect(self) -> Iterator[RentObservation]:
        """
        Yield rent observations for all localities.

        Discovers the working API endpoint first, then iterates over all
        locality codes.
        """
        if self._active_endpoint is None:
            self.discover_endpoint()

        crosswalk = get_crosswalk()
        codes = self._locality_codes or crosswalk.all_codes()

        for code in track(codes, description="Fetching nadlan.gov.il…"):
            locality = crosswalk.by_code(code)
            if locality is None:
                logger.warning("Unknown locality code: %s", code)
                continue

            if self.dry_run:
                logger.info("[dry-run] Would fetch locality %s (%s)", code, locality.name_he)
                continue

            try:
                observations = list(self._fetch_locality(code, locality.name_he, locality.name_en))
                yield from observations
                if not observations:
                    logger.debug("No data for locality %s (%s)", code, locality.name_he)
            except Exception as exc:
                logger.warning(
                    "Failed to fetch locality %s (%s): %s", code, locality.name_he, exc
                )

    # ------------------------------------------------------------------
    # Per-locality fetch
    # ------------------------------------------------------------------

    def _fetch_locality(
        self, code: str, name_he: str, name_en: str
    ) -> Iterator[RentObservation]:
        """Fetch and parse rent data for a single locality."""
        if self._active_endpoint:
            yield from self._fetch_via_api(code, name_he, name_en)
        else:
            yield from self._fetch_via_html(code, name_he, name_en)

    def _fetch_via_api(
        self, code: str, name_he: str, name_en: str
    ) -> Iterator[RentObservation]:
        pattern = self._active_endpoint
        assert pattern is not None
        url_path = pattern.replace("{id}", code).replace("PROXY:", "")
        base = _base_url_for_pattern(pattern)
        url = f"{base}{url_path}"

        client = get_client()
        resp = client.get(url, params=_build_params(code, pattern), raise_for_status=False)

        if resp.status_code != 200:
            return

        try:
            data = resp.json()
        except Exception:
            return

        yield from _parse_response(data, code, name_he, name_en)

    def _fetch_via_html(
        self, code: str, name_he: str, name_en: str
    ) -> Iterator[RentObservation]:
        """
        Fallback: parse the public rental-trends HTML page for a locality.

        The page URL is:
            https://www.nadlan.gov.il/?id={code}&page=rent&view=settlement_rent

        The rental statistics are injected as a JSON blob inside a <script> tag
        with id="__NEXT_DATA__" (Next.js convention) or embedded as a JS variable.

        EXECUTOR NOTE: If this fallback is needed, inspect the page source of
            https://www.nadlan.gov.il/?id=5000&page=rent&view=settlement_rent
        and update _parse_html_page() accordingly.
        """
        from bs4 import BeautifulSoup
        import json
        import re

        url = f"{NADLAN_BASE_URL}/?id={code}&page=rent&view=settlement_rent"
        client = get_client()

        try:
            resp = client.get(url, raise_for_status=False)
        except Exception as exc:
            logger.debug("HTML fetch failed for %s: %s", code, exc)
            return

        if resp.status_code != 200:
            return

        soup = BeautifulSoup(resp.text, "lxml")

        # Try Next.js data blob first
        next_data_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if next_data_tag and next_data_tag.string:
            try:
                next_data = json.loads(next_data_tag.string)
                yield from _parse_nextjs_blob(next_data, code, name_he, name_en)
                return
            except Exception:
                pass

        # Try inline JS variable (e.g. window.__DATA__ = {...})
        for script in soup.find_all("script"):
            text = script.string or ""
            # Look for JSON-like objects containing rent data
            match = re.search(r"window\.__(?:DATA|APP_STATE|RENT_DATA)__\s*=\s*(\{.*?\});", text, re.DOTALL)
            if match:
                try:
                    blob = json.loads(match.group(1))
                    yield from _parse_response(blob, code, name_he, name_en)
                    return
                except Exception:
                    pass

        logger.debug("HTML fallback: could not extract data for locality %s", code)

    # ------------------------------------------------------------------
    # Probe (fast check)
    # ------------------------------------------------------------------

    def probe(self) -> dict[str, object]:
        endpoint = self.discover_endpoint()
        return {
            "ok": endpoint is not None,
            "active_endpoint": endpoint,
            "fallback": "html" if endpoint is None else None,
        }


# ---------------------------------------------------------------------------
# Helpers: resolve base URL and build request params
# ---------------------------------------------------------------------------


def _base_url_for_pattern(pattern: str) -> str:
    """
    Return the correct base URL for an endpoint pattern.

    Patterns prefixed with 'PROXY:' use the known proxy host
    (proxy-nadlan.taxes.gov.il); all others use the main nadlan.gov.il domain.
    """
    if pattern.startswith("/pages/"):
        return NADLAN_DATA_BASE_URL
    from rent_collector.config import NADLAN_PROXY_BASE_URL
    if pattern.startswith("PROXY:"):
        return NADLAN_PROXY_BASE_URL
    return NADLAN_BASE_URL


def _build_params(code: str, pattern: str) -> dict[str, Any]:
    """Build query params for a given endpoint pattern and locality code."""
    year = NADLAN_TARGET_YEAR or 2025
    quarter = NADLAN_TARGET_QUARTER or 4

    # Pattern 1: /api/getRentsBySettlement  (query-string style)
    if pattern.startswith("/pages/"):
        return {}

    if "getRentsBySettlement" in pattern:
        return {
            "id": code,
            "year": year,
            "quarter": quarter,
        }

    # Pattern 2: /api/settlement/{id}/rent  (URL param, maybe date filters)
    if "/settlement/" in pattern:
        return {
            "year": year,
            "quarter": quarter,
        }

    # Pattern 3: /api/RentAnalysis/GetRentBySettlementCode
    if "RentAnalysis" in pattern or "GetRent" in pattern:
        return {
            "settlementCode": code,
            "year": year,
            "quarter": quarter,
        }

    # Pattern 4: /NadlanAPI/GetRentBySettlementCode (older Tax Authority style)
    if "NadlanAPI" in pattern:
        return {
            "settlementCode": code,
            "fromDate": f"{year - 1}-01-01",
            "toDate": f"{year}-12-31",
        }

    return {"id": code, "year": year, "quarter": quarter}


# ---------------------------------------------------------------------------
# Helpers: response shape detection and parsing
# ---------------------------------------------------------------------------


def _looks_like_rent_data(data: Any) -> bool:
    """Heuristic: does this JSON response look like it contains rent data?"""
    if isinstance(data, dict):
        trends = data.get("trends")
        if isinstance(trends, dict) and isinstance(trends.get("rooms"), list):
            return True
    if not isinstance(data, (dict, list)):
        return False
    text = str(data).lower()
    # Look for Hebrew or English rent-related keywords
    rent_keywords = ["rent", "שכר", "חדר", "median", "average", "price", "שכירות", "דירה"]
    return any(k in text for k in rent_keywords) and len(text) > 50


def _parse_response(
    data: Any, code: str, name_he: str, name_en: str
) -> Iterator[RentObservation]:
    """
    Parse a JSON response from nadlan.gov.il into RentObservation instances.

    Handles several known response shapes:
      Shape A: list of {rooms, median, avg, count, quarter, year}
      Shape B: {data: [{rooms, rent, ...}]}
      Shape C: {rentByRooms: {2: {median: X}, 3: {median: Y}}}
      Shape D: nested Next.js pageProps structure

    EXECUTOR NOTE: After probing the live endpoint, add the actual shape here
    and remove the shapes that don't match.
    """
    year = NADLAN_TARGET_YEAR or 2025
    quarter = NADLAN_TARGET_QUARTER or 4

    # Live shape as of 2026-04-21:
    # {"settlementID": 5000, "settlementName": "...", "trends": {"rooms": [
    #   {"numRooms": 3, "summary": {"lastYearAvgPrice": 7999}, "graphData": [...]},
    #   ...
    # ]}}
    if isinstance(data, dict):
        trends = data.get("trends")
        rooms = trends.get("rooms") if isinstance(trends, dict) else None
        if isinstance(rooms, list):
            for item in rooms:
                if not isinstance(item, dict):
                    continue
                room_group = _parse_room_group(str(item.get("numRooms")))
                if room_group is None:
                    continue
                summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
                graph_data = item.get("graphData") if isinstance(item.get("graphData"), list) else []
                point = next(
                    (
                        row for row in graph_data
                        if isinstance(row, dict) and row.get("settlementPrice")
                    ),
                    None,
                )
                avg = _extract_price(summary, ["lastYearAvgPrice", "settlementPrice"])
                if avg is None and point is not None:
                    avg = _extract_price(point, ["settlementPrice"])
                if avg is None:
                    continue

                obs_year = int(point.get("year")) if point and point.get("year") else year
                obs_quarter = (
                    ((int(point.get("month")) - 1) // 3 + 1)
                    if point and point.get("month")
                    else quarter
                )
                yield RentObservation(
                    locality_code=code,
                    locality_name_he=name_he,
                    locality_name_en=name_en,
                    room_group=room_group,
                    median_rent_nis=avg,
                    avg_rent_nis=avg,
                    rent_nis=avg,
                    source=DataSource.NADLAN,
                    year=obs_year,
                    quarter=obs_quarter,
                    notes="nadlan settlement rent page JSON",
                )
            return

    # Shape A: top-level list
    if isinstance(data, list):
        for item in data:
            obs = _item_to_observation(item, code, name_he, name_en, year, quarter)
            if obs:
                yield obs
        return

    # Shape B: {"data": [...]}
    if isinstance(data, dict):
        for key in ("data", "result", "results", "items", "rentData", "rents"):
            inner = data.get(key)
            if isinstance(inner, list):
                for item in inner:
                    obs = _item_to_observation(item, code, name_he, name_en, year, quarter)
                    if obs:
                        yield obs
                return

        # Shape C: {"rentByRooms": {"2": {"median": X}, ...}}
        for key in ("rentByRooms", "byRooms", "roomGroups", "rooms"):
            inner = data.get(key)
            if isinstance(inner, dict):
                for room_key, stats in inner.items():
                    if not isinstance(stats, dict):
                        continue
                    room_group = _parse_room_group(room_key)
                    if room_group is None:
                        continue
                    median = _extract_price(stats, ["median", "medianPrice", "medianRent"])
                    avg = _extract_price(stats, ["average", "avg", "avgPrice", "avgRent", "meanRent"])
                    if median is None and avg is None:
                        continue
                    yield _make_observation(
                        code, name_he, name_en, room_group, median, avg, year, quarter,
                        count=stats.get("count") or stats.get("numTransactions"),
                    )
                return

    # Unknown shape — log a sample for the executor to investigate
    logger.warning(
        "Unrecognised nadlan response shape for locality %s. "
        "Sample: %s … Update _parse_response() after inspecting.",
        code,
        str(data)[:300],
    )


def _parse_nextjs_blob(
    blob: dict[str, Any], code: str, name_he: str, name_en: str
) -> Iterator[RentObservation]:
    """Parse data extracted from a Next.js __NEXT_DATA__ script tag."""
    # Navigate typical Next.js structure: props → pageProps → data/rent/…
    try:
        page_props = blob["props"]["pageProps"]
    except (KeyError, TypeError):
        return

    for key in ("rentData", "rent", "data", "settlementRent"):
        inner = page_props.get(key)
        if inner is not None:
            yield from _parse_response(inner, code, name_he, name_en)
            return


def _item_to_observation(
    item: Any,
    code: str,
    name_he: str,
    name_en: str,
    year: int,
    quarter: int,
) -> RentObservation | None:
    """Convert a single dict item from a list response to a RentObservation."""
    if not isinstance(item, dict):
        return None

    # Normalise room key
    room_raw = (
        item.get("rooms")
        or item.get("numRooms")
        or item.get("roomCount")
        or item.get("חדרים")
        or item.get("מספר_חדרים")
    )
    room_group = _parse_room_group(str(room_raw)) if room_raw is not None else None
    if room_group is None:
        return None

    median = _extract_price(item, ["median", "medianPrice", "medianRent", "חציון"])
    avg = _extract_price(item, ["average", "avg", "avgPrice", "avgRent", "ממוצע"])

    if median is None and avg is None:
        return None

    obs_year = item.get("year") or item.get("שנה") or year
    obs_quarter = item.get("quarter") or item.get("רבעון") or quarter
    count = item.get("count") or item.get("numTransactions") or item.get("transactions")

    return _make_observation(
        code, name_he, name_en, room_group, median, avg,
        int(obs_year), int(obs_quarter), count=count,
    )


def _make_observation(
    code: str,
    name_he: str,
    name_en: str,
    room_group: RoomGroup,
    median: float | None,
    avg: float | None,
    year: int,
    quarter: int,
    count: Any = None,
) -> RentObservation:
    best = median if median is not None else avg
    assert best is not None
    return RentObservation(
        locality_code=code,
        locality_name_he=name_he,
        locality_name_en=name_en,
        room_group=room_group,
        median_rent_nis=median,
        avg_rent_nis=avg,
        rent_nis=best,
        source=DataSource.NADLAN,
        year=year,
        quarter=quarter,
        observations_count=int(count) if count else None,
    )


def _parse_room_group(raw: str) -> RoomGroup | None:
    """Map a raw room-count label to a RoomGroup enum."""
    raw = str(raw).strip()
    if raw in _ROOM_LABEL_MAP:
        return _ROOM_LABEL_MAP[raw]
    try:
        val = float(raw)
        return RoomGroup.from_float(val)
    except ValueError:
        return None


def _extract_price(d: dict[str, Any], keys: list[str]) -> float | None:
    for k in keys:
        v = d.get(k)
        if v is not None:
            try:
                f = float(v)
                if f > 0:
                    return f
            except (TypeError, ValueError):
                pass
    return None
