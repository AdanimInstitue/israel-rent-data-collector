"""
Bank of Israel Hedonic Rent Model.

This module implements a fallback rent estimator for localities that have no
data from nadlan.gov.il or CBS Table 4.9.

The model is based on the hedonic regression published in:
  "The Changes in Rent in Israel During the Years 2008–2015"
  Bank of Israel Research Department
  URL: https://www.boi.org.il/media/yulnw1sl/part-3n.pdf

Model form:
  log(rent_it) = α + β_rooms * rooms + Σ γ_city * city_dummy_i + time_effects + ε

For our use case we only need the steady-state cross-sectional component:
  log(rent) ≈ α + β_rooms * rooms + γ_city
  rent ≈ exp(α + β_rooms * rooms + γ_city)

EXECUTOR NOTE:
  1. Download the paper from BOI_HEDONIC_PAPER_URL.
  2. Find the regression table (usually "Table A" or "Appendix").
  3. Extract:
     - The intercept (α)
     - The coefficient on number of rooms (β_rooms)
     - City fixed effects (γ_city) for as many cities as listed
  4. Replace the placeholder values in _PLACEHOLDER_COEFFICIENTS below with
     the actual values from the paper.
  5. Set COEFFICIENTS_ARE_PLACEHOLDER = False.
  6. Run `python scripts/collect.py --source boi-hedonic --dry-run` to verify.

Alternative: if the paper only publishes elasticities or index values (not
absolute rent levels), the model can be calibrated using CBS Table 4.9 for
the cities that do appear there, then extrapolated to smaller cities via the
relative city effects.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterator

from rich.console import Console

from rent_collector.collectors.base import BaseCollector
from rent_collector.config import BOI_HEDONIC_PAPER_URL
from rent_collector.models import BoIHedonicCoefficients, DataSource, RentObservation, RoomGroup
from rent_collector.utils.locality_crosswalk import get_crosswalk

logger = logging.getLogger(__name__)
console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Coefficient store
# ---------------------------------------------------------------------------

# The published paper exposes the fixed-hedonics intercept and room effects in
# Appendix B, Table 8. It does not enumerate the municipality dummy
# coefficients, so the city effects below remain a data-derived approximation
# anchored to official rent data rather than copied from the article.
COEFFICIENTS_ARE_PLACEHOLDER = False

# Reference city: Tel Aviv-Yafo (code 5000); its effect = 0.0 by convention.
# All other city effects are relative to Tel Aviv.

_PLACEHOLDER_COEFFICIENTS = BoIHedonicCoefficients(
    # Appendix B, Table 8 reports a constant of 7.1524 and room dummies for
    # 3/4/5/6 rooms. The collector uses a simplified linear room term, so we
    # fit a straight line through the published room effects and fold the offset
    # into the intercept:
    #   baseline log intercept ~= 6.8650
    #   beta_rooms ~= 0.1574
    intercept=6.8650,
    beta_rooms=0.1574,
    beta_floor_area=None,  # floor area not used in our simplified model
    beta_floor_number=None,
    beta_building_age=None,
    city_effects={
        # Published article does not print the municipality coefficients.
        # These locality effects are derived from official nadlan 3-room rent
        # differentials relative to Tel Aviv to keep the fallback useful.
        "3000": -0.3837,
        "4000": -0.8854,
        "5000": 0.00,  # Tel Aviv (reference)
        "6100": -0.5533,
        "6200": -0.5642,
        "6300": -0.2467,
        "6500": -0.8266,
        "6700": -1.1238,
        "6900": -0.4699,
        "7400": -0.5864,
        "7900": -0.5977,
        "8200": -0.9162,
        "8300": -0.5003,
        "8600": -0.3044,
        "9000": -1.1335,
        "9100": -0.9975,
        "9200": -1.3987,
    },
    reference_city_code="5000",
    reference_year=2015,
    paper_url=BOI_HEDONIC_PAPER_URL,
)

# ---------------------------------------------------------------------------
# Rent-level calibration (2025 adjustment)
# ---------------------------------------------------------------------------

# CBS-published average 3-room rent for Tel Aviv (2024 Q4) in NIS.
# Used to calibrate the model's absolute level.
# Update after CBS Table 4.9 data is fetched.
TEL_AVIV_3ROOM_REFERENCE_NIS: float = 7200.0  # approximate 2025 figure


class BoIHedonicCollector(BaseCollector):
    """
    Predict rent for all localities using the Bank of Israel hedonic model.

    This is a *fallback* — use only for localities with no nadlan/CBS data.
    The model is calibrated so that Tel Aviv 3-room matches the CBS reference.
    """

    name = "boi_hedonic"

    def __init__(
        self,
        *,
        dry_run: bool = False,
        coefficients: BoIHedonicCoefficients | None = None,
        only_for_missing_localities: bool = True,
        known_localities: set[str] | None = None,
    ) -> None:
        super().__init__(dry_run=dry_run)
        self._coef = coefficients or _PLACEHOLDER_COEFFICIENTS
        self._only_missing = only_for_missing_localities
        self._known = known_localities or set()

    def predict(self, locality_code: str, room_group: RoomGroup) -> float:
        """
        Predict monthly rent (NIS) for a locality + room group.

        If the locality code is not in the model's city_effects dict, we use
        the national average effect (0 = Tel Aviv level, then scaled down by
        mean city effect across all cities).
        """
        rooms = _room_group_to_float(room_group)
        city_effect = self._coef.city_effects.get(locality_code, _mean_city_effect(self._coef))

        log_rent_baseline = self._coef.intercept + self._coef.beta_rooms * rooms + city_effect
        rent_baseline = math.exp(log_rent_baseline)

        # Calibrate absolute level: adjust so TA 3-room matches reference
        ta_3room_log = self._coef.intercept + self._coef.beta_rooms * 3.0  # TA effect = 0
        ta_3room_baseline = math.exp(ta_3room_log)
        calibration_factor = TEL_AVIV_3ROOM_REFERENCE_NIS / ta_3room_baseline

        return round(rent_baseline * calibration_factor, 0)

    def collect(self) -> Iterator[RentObservation]:
        if COEFFICIENTS_ARE_PLACEHOLDER:
            console.log(
                "[yellow]BoI hedonic model: using PLACEHOLDER coefficients. "
                "Run executor to extract real values from the BoI paper.[/yellow]"
            )

        if self.dry_run:
            console.log("[dim][dry-run] BoI hedonic collect skipped.[/dim]")
            return

        crosswalk = get_crosswalk()
        room_groups = [
            RoomGroup.R2_0,
            RoomGroup.R2_5,
            RoomGroup.R3_0,
            RoomGroup.R3_5,
            RoomGroup.R4_0,
            RoomGroup.R4_5,
            RoomGroup.R5_0,
            RoomGroup.R5_PLUS,
        ]

        for locality in crosswalk.all_localities():
            code = locality.code
            if self._only_missing and code in self._known:
                continue

            for rg in room_groups:
                try:
                    rent = self.predict(code, rg)
                except Exception as exc:
                    logger.debug("BoI predict failed for %s %s: %s", code, rg, exc)
                    continue

                yield RentObservation(
                    locality_code=code,
                    locality_name_he=locality.name_he,
                    locality_name_en=locality.name_en,
                    room_group=rg,
                    rent_nis=rent,
                    source=DataSource.BOI_HEDONIC,
                    year=2025,
                    quarter=None,
                    notes="placeholder-coefficients" if COEFFICIENTS_ARE_PLACEHOLDER else "",
                )

    @classmethod
    def download_paper(cls) -> bytes | None:
        """Download the BoI hedonic paper PDF for coefficient extraction."""
        from rent_collector.utils.http_client import get_client

        client = get_client()
        try:
            content = client.get_bytes(BOI_HEDONIC_PAPER_URL)
            console.log(f"[green]BoI hedonic paper downloaded ({len(content):,} bytes)[/green]")
            return content
        except Exception as exc:
            logger.error("BoI paper download failed: %s", exc)
            return None

    def probe(self) -> dict[str, object]:
        try:
            rent = self.predict("5000", RoomGroup.R3_0)
            return {
                "ok": True,
                "coefficients_are_placeholder": COEFFICIENTS_ARE_PLACEHOLDER,
                "ta_3room_predicted_nis": rent,
                "paper_url": BOI_HEDONIC_PAPER_URL,
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _room_group_to_float(rg: RoomGroup) -> float:
    if rg == RoomGroup.R5_PLUS:
        return 6.0  # use 6 as representative of "5+"
    return float(rg.value)


def _mean_city_effect(coef: BoIHedonicCoefficients) -> float:
    """Compute mean city effect across all cities (for unknown localities)."""
    if not coef.city_effects:
        return -0.20  # default: ~18% cheaper than reference city
    return sum(coef.city_effects.values()) / len(coef.city_effects)
