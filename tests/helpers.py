from __future__ import annotations

from rent_collector.models import Locality
from rent_collector.utils.locality_crosswalk import LocalityCrosswalk


def make_crosswalk() -> LocalityCrosswalk:
    return LocalityCrosswalk(
        [
            Locality(code="5000", name_he="תל אביב - יפו", name_en="Tel Aviv - Yafo"),
            Locality(code="9000", name_he="באר שבע", name_en="Beer Sheva"),
        ]
    )
