"""
Shared models for the public-safe reference-data collector.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class Locality(BaseModel):
    """An Israeli locality from the CBS registry."""

    code: str = Field(description="CBS 4-digit locality code.")
    name_he: str = Field(description="Hebrew name.")
    name_en: str = Field(default="", description="English transliteration.")
    district_he: str = Field(default="", description="District (מחוז) in Hebrew.")
    district_en: str = Field(default="", description="District in English.")
    sub_district_he: str = Field(default="", description="Sub-district (נפה) in Hebrew.")
    population: int | None = Field(default=None)
    is_municipal_authority: bool = Field(default=False)
    source: str = Field(default="data.gov.il", description="Source of the locality record.")
