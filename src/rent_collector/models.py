"""
Shared data models for the rent collector.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class DataSource(str, Enum):
    """Which collector produced this observation."""

    NADLAN = "nadlan.gov.il"
    CBS_API = "cbs_api"
    CBS_TABLE49 = "cbs_table49"
    BOI_HEDONIC = "boi_hedonic"
    SEED = "seed_csv"  # offline fallback from the bundled seed CSV


class RoomGroup(str, Enum):
    """
    Israeli apartment room-count convention.

    Rooms include living room + bedrooms (Israeli standard).
    Half-rooms (e.g. 2.5) are common; they represent a small study/office.
    """

    R1_0 = "1.0"
    R1_5 = "1.5"
    R2_0 = "2.0"
    R2_5 = "2.5"
    R3_0 = "3.0"
    R3_5 = "3.5"
    R4_0 = "4.0"
    R4_5 = "4.5"
    R5_0 = "5.0"
    R5_PLUS = "5+"

    @classmethod
    def from_float(cls, value: float) -> "RoomGroup":
        """Convert a numeric value to a RoomGroup, rounding to nearest 0.5."""
        rounded = round(value * 2) / 2
        key = f"{rounded:.1f}" if rounded < 5 else "5+"
        try:
            return cls(key)
        except ValueError:
            return cls.R5_PLUS


# ---------------------------------------------------------------------------
# Core output model
# ---------------------------------------------------------------------------


class RentObservation(BaseModel):
    """
    A single rent observation: the estimated/reported rent for a given
    (locality, room_group) combination.
    """

    locality_code: str = Field(
        description="CBS 4-digit locality code (מספר ישוב). E.g. '5000' for Tel Aviv."
    )
    locality_name_he: str = Field(
        description="Locality name in Hebrew. E.g. 'תל אביב-יפו'."
    )
    locality_name_en: str = Field(
        default="",
        description="Locality name in English (transliterated). E.g. 'Tel Aviv-Yafo'.",
    )
    room_group: RoomGroup = Field(
        description="Apartment room-count category (Israeli convention)."
    )
    median_rent_nis: float | None = Field(
        default=None,
        description="Median monthly rent in NIS. Available from nadlan.gov.il.",
    )
    avg_rent_nis: float | None = Field(
        default=None,
        description="Average monthly rent in NIS. Available from CBS Table 4.9 / API.",
    )
    rent_nis: float = Field(
        description=(
            "Best available rent estimate in NIS: median if available, else average."
        )
    )
    source: DataSource = Field(description="Which collector produced this observation.")
    quarter: int | None = Field(
        default=None, ge=1, le=4, description="Quarter (1-4) this data refers to."
    )
    year: int | None = Field(
        default=None, ge=2015, le=2030, description="Year this data refers to."
    )
    observations_count: int | None = Field(
        default=None,
        description="Number of lease contracts underlying this estimate (if reported).",
    )
    notes: str = Field(default="", description="Any caveats or flags for this row.")

    @field_validator("rent_nis", mode="before")
    @classmethod
    def _set_best_rent(cls, v: float | None, info: object) -> float:
        """If rent_nis not set, use median > average."""
        if v is not None:
            return v
        data = info.data if hasattr(info, "data") else {}
        median = data.get("median_rent_nis")
        avg = data.get("avg_rent_nis")
        if median is not None:
            return median
        if avg is not None:
            return avg
        raise ValueError("At least one of median_rent_nis or avg_rent_nis must be set.")


# ---------------------------------------------------------------------------
# Locality model
# ---------------------------------------------------------------------------


class Locality(BaseModel):
    """An Israeli locality from the CBS registry."""

    code: str = Field(description="CBS 4-digit locality code.")
    name_he: str = Field(description="Hebrew name.")
    name_en: str = Field(default="", description="English transliteration.")
    district_he: str = Field(default="", description="District (מחוז) in Hebrew.")
    sub_district_he: str = Field(
        default="", description="Sub-district (נפה) in Hebrew."
    )
    population: int | None = Field(default=None)
    is_municipal_authority: bool = Field(default=False)


# ---------------------------------------------------------------------------
# BoI hedonic model coefficients
# ---------------------------------------------------------------------------


class BoIHedonicCoefficients(BaseModel):
    """
    Regression coefficients from the Bank of Israel hedonic rent model.

    Model form:  log(rent) = intercept + β_rooms * rooms + Σ β_city_i * city_i + ε

    All coefficients are log-scale; exponentiate to get multiplicative factors
    relative to the reference city.

    Source: "The Changes in Rent in Israel During the Years 2008–2015",
    Bank of Israel Research Department.
    """

    intercept: float = Field(description="Log-scale intercept.")
    beta_rooms: float = Field(description="Coefficient on number of rooms.")
    beta_floor_area: float | None = Field(
        default=None, description="Coefficient on floor area (sq m), if in the model."
    )
    beta_floor_number: float | None = Field(
        default=None, description="Coefficient on floor number."
    )
    beta_building_age: float | None = Field(
        default=None, description="Coefficient on building age (years)."
    )
    # City fixed effects: dict from locality_code → coefficient
    # Reference city (coefficient = 0.0) is noted in reference_city_code.
    city_effects: dict[str, float] = Field(
        default_factory=dict,
        description="Log-scale city fixed effects relative to reference city.",
    )
    reference_city_code: str = Field(
        default="5000", description="CBS code of the reference city (coefficient = 0)."
    )
    reference_year: int = Field(
        default=2015, description="Year the regression was estimated."
    )
    paper_url: str = Field(default="", description="Source paper URL.")
