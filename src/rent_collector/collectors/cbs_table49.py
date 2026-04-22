"""
CBS Table 4.9 collector — Average Monthly Prices of Rent.

Downloads the Excel (preferred) or PDF version of CBS Table 4.9 from the
monthly Consumer Price Statistics publication and parses it into
RentObservation instances.

Table 4.9 columns (typical):
  City / district | 2 rooms | 2.5 rooms | 3 rooms | 3.5 rooms | 4 rooms | 4.5 rooms | 5+ rooms

Rows are cities (Tel Aviv, Haifa, Jerusalem, etc.) and districts
(North, South, etc.).

URL template:
  Excel: https://www.cbs.gov.il/he/publications/Madad/DocLib/{year}/price{month:02d}{letter}/a4_9_e.xlsx
  PDF:   https://www.cbs.gov.il/he/publications/Madad/DocLib/{year}/price{month:02d}{letter}/a4_9_e.pdf

EXECUTOR NOTE:
  If the URL template fails (CBS may reorganise paths), search for "a4_9" on:
    https://www.cbs.gov.il/en/subjects/Pages/Average-Monthly-Prices-of-Rent.aspx
  to find the current download link.
"""

from __future__ import annotations

import io
import logging
import re
from collections.abc import Iterator

import pandas as pd
from rich.console import Console

from rent_collector.collectors.base import BaseCollector
from rent_collector.config import (
    CBS_TABLE49_LATEST_LETTER,
    CBS_TABLE49_LATEST_MONTH,
    CBS_TABLE49_LATEST_YEAR,
    CBS_TABLE49_PDF_URL_TEMPLATE,
    CBS_TABLE49_URL_TEMPLATE,
)
from rent_collector.models import DataSource, RentObservation, RoomGroup
from rent_collector.utils.http_client import get_client
from rent_collector.utils.locality_crosswalk import get_crosswalk

logger = logging.getLogger(__name__)
console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Known CBS locality/district name → CBS locality code mapping for Table 4.9.
# Table 4.9 uses Hebrew city/district labels; we need to map them to codes.
# ---------------------------------------------------------------------------

# Cities that appear in Table 4.9 with their CBS codes
_TABLE49_SECTION_LABELS = {"Residential Districts", "Big cities"}

_TABLE49_ROOM_ROWS: dict[str, RoomGroup] = {
    "1-2": RoomGroup.R2_0,
    "2.5-3": RoomGroup.R3_0,
    "3.5-4": RoomGroup.R4_0,
    "4.5-6": RoomGroup.R5_PLUS,
}

_TABLE49_LOCATION_ALIASES: dict[str, str] = {
    "ashdod": "70",
    "ashkelon": "7100",
    "bat yam": "6200",
    "beer sheva": "9000",
    "bet shemesh": "2610",
    "bnei brak": "6100",
    "hadera": "6500",
    "haifa": "4000",
    "herzlliya": "6400",
    "holon": "6600",
    "jerusalem": "3000",
    "kfar saba": "6900",
    "netanya": "7400",
    "petah tiqwa": "7900",
    "ramat gan": "8600",
    "rehovot": "8400",
    "rishon lezion": "8300",
    "tel aviv": "5000",
}

_TABLE49_DISTRICT_LABELS: dict[str, tuple[str, str]] = {
    "jerusalem district": ("DIST_JER", "מחוז ירושלים"),
    "north district": ("DIST_NORTH", "מחוז הצפון"),
    "haifa district": ("DIST_HAIFA", "מחוז חיפה"),
    "center district": ("DIST_CENTER", "מחוז המרכז"),
    "tel aviv district": ("DIST_TA", "מחוז תל אביב"),
    "south district": ("DIST_SOUTH", "מחוז הדרום"),
}


class CBSTable49Collector(BaseCollector):
    """Download and parse CBS Table 4.9 from the monthly CPI publication."""

    name = "cbs_table49"

    def __init__(
        self,
        *,
        dry_run: bool = False,
        year: int = CBS_TABLE49_LATEST_YEAR,
        month: int = CBS_TABLE49_LATEST_MONTH,
        letter: str = CBS_TABLE49_LATEST_LETTER,
    ) -> None:
        super().__init__(dry_run=dry_run)
        self._year = year
        self._month = month
        self._letter = letter

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _excel_url(self) -> str:
        return CBS_TABLE49_URL_TEMPLATE.format(
            year=self._year, month=self._month, letter=self._letter
        )

    def _pdf_url(self) -> str:
        return CBS_TABLE49_PDF_URL_TEMPLATE.format(
            year=self._year, month=self._month, letter=self._letter
        )

    def _download_excel(self) -> bytes | None:
        """Download the Excel file. Returns None if not found."""
        client = get_client()
        url = self._excel_url()
        try:
            resp = client.get(url, raise_for_status=False)
            if resp.status_code == 200:
                console.log(f"[green]CBS Table 4.9 Excel downloaded from {url}[/green]")
                return bytes(resp.content)
            console.log(f"[yellow]CBS Table 4.9 Excel: HTTP {resp.status_code} at {url}[/yellow]")
        except Exception as exc:
            logger.debug("Excel download failed: %s", exc)
        return None

    def _download_pdf(self) -> bytes | None:
        """Download the PDF as a fallback."""
        client = get_client()
        url = self._pdf_url()
        try:
            resp = client.get(url, raise_for_status=False)
            if resp.status_code == 200:
                console.log(f"[green]CBS Table 4.9 PDF downloaded from {url}[/green]")
                return bytes(resp.content)
        except Exception as exc:
            logger.debug("PDF download failed: %s", exc)
        return None

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_excel(self, content: bytes) -> pd.DataFrame:
        """Parse the downloaded Excel file into a tidy DataFrame."""
        df = pd.read_excel(io.BytesIO(content), sheet_name=0, header=None)
        value_col, year, quarter = _latest_price_column(df)
        return _extract_table49_entities(df, value_col=value_col, year=year, quarter=quarter)

    def _parse_pdf(self, content: bytes) -> pd.DataFrame:
        """Parse the PDF using pdfplumber."""
        import pdfplumber

        rows: list[list[str | None]] = []
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    rows.extend(table)
        if not rows:
            raise ValueError("No table found in CBS Table 4.9 PDF.")

        raise ValueError("CBS Table 4.9 PDF parsing is not implemented for the current layout.")

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def collect(self) -> Iterator[RentObservation]:
        if self.dry_run:
            console.log(
                f"[dim][dry-run] Would download CBS Table 4.9 from {self._excel_url()}[/dim]"
            )
            return

        content = self._download_excel()
        format_used = "excel"
        if content is None:
            content = self._download_pdf()
            format_used = "pdf"
        if content is None:
            logger.error("CBS Table 4.9: both Excel and PDF download failed.")
            return

        try:
            df = self._parse_excel(content) if format_used == "excel" else self._parse_pdf(content)
        except Exception as exc:
            logger.error("CBS Table 4.9 parse failed: %s", exc)
            return

        for _, row in df.iterrows():
            city_label = str(row.get("city", "")).strip()
            if not city_label:
                continue

            code, name_he, name_en = _resolve_table49_location(city_label)

            avg_rent = float(row["avg_rent_nis"])
            yield RentObservation(
                locality_code=code,
                locality_name_he=name_he,
                locality_name_en=name_en,
                room_group=RoomGroup(row["room_group"]),
                avg_rent_nis=avg_rent,
                rent_nis=avg_rent,
                source=DataSource.CBS_TABLE49,
                year=int(row["year"]),
                quarter=int(row["quarter"]),
            )

    def probe(self) -> dict[str, object]:
        content = self._download_excel()
        if content:
            return {"ok": True, "format": "excel", "url": self._excel_url()}
        content = self._download_pdf()
        if content:
            return {"ok": True, "format": "pdf", "url": self._pdf_url()}
        return {"ok": False, "tried_excel": self._excel_url(), "tried_pdf": self._pdf_url()}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _latest_price_column(df: pd.DataFrame) -> tuple[int, int, int]:
    """Return the rightmost populated average-price column and its (year, quarter)."""
    best: tuple[int, int, int] | None = None
    quarter_map = {
        "I-III": 1,
        "IV-VI": 2,
        "VII-IX": 3,
        "X-XII": 4,
        "Annual\naverage": 4,
    }
    current_year: int | None = None
    for col in range(1, df.shape[1]):
        if "Average" not in str(df.iloc[5, col]):
            continue
        year_raw = df.iloc[3, col]
        if not pd.isna(year_raw):
            current_year = int(year_raw)
        period_raw = str(df.iloc[4, col]).strip()
        if current_year is None or period_raw not in quarter_map:
            continue
        if pd.isna(df.iloc[7, col]):
            continue
        candidate = (col, current_year, quarter_map[period_raw])
        if best is None or (candidate[1], candidate[2], candidate[0]) > (
            best[1],
            best[2],
            best[0],
        ):
            best = candidate
    if best is None:
        raise ValueError("Could not find a populated average-price column in Table 4.9.")
    return best


def _extract_table49_entities(
    df: pd.DataFrame,
    *,
    value_col: int,
    year: int,
    quarter: int,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    current_entity: str | None = None

    for idx in range(7, len(df)):
        label_raw = df.iloc[idx, 0]
        if pd.isna(label_raw):
            continue
        label = str(label_raw).strip()
        if not label or label in _TABLE49_SECTION_LABELS or label.startswith("("):
            continue
        if label == "Total":
            current_entity = None
            continue
        if label in _TABLE49_ROOM_ROWS:
            if current_entity is None:
                continue
            value = df.iloc[idx, value_col]
            if pd.isna(value) or str(value).strip() == "-":
                continue
            rows.append(
                {
                    "city": current_entity,
                    "room_group": _TABLE49_ROOM_ROWS[label].value,
                    "avg_rent_nis": float(value),
                    "year": year,
                    "quarter": quarter,
                }
            )
            continue
        current_entity = _clean_table49_label(label)

    return pd.DataFrame(rows)


def _clean_table49_label(label: str) -> str:
    label = re.sub(r"\s*-\s*\d+\s*$", "", label).strip()
    return label


def _resolve_table49_location(city_label: str) -> tuple[str, str, str]:
    crosswalk = get_crosswalk()

    code_match = re.search(r"(\d{2,4})$", city_label)
    if code_match:
        loc = crosswalk.by_code(code_match.group(1))
        if loc:
            return loc.code, loc.name_he, loc.name_en

    cleaned = _clean_table49_label(city_label)
    loc = crosswalk.by_name(cleaned)
    if loc:
        return loc.code, loc.name_he, loc.name_en
    loc = crosswalk.by_name_en(cleaned)
    if loc:
        return loc.code, loc.name_he, loc.name_en

    normalized = cleaned.lower()
    alias_code = _TABLE49_LOCATION_ALIASES.get(normalized)
    if alias_code:
        loc = crosswalk.by_code(alias_code)
        if loc:
            return loc.code, loc.name_he, loc.name_en

    district = _TABLE49_DISTRICT_LABELS.get(normalized)
    if district:
        district_code, district_name_he = district
        return district_code, district_name_he, cleaned

    return f"UNKNOWN_{cleaned}", cleaned, cleaned
