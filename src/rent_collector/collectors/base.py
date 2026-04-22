"""
Abstract base class for all rent collectors.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator

from rent_collector.models import RentObservation

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """
    Every collector yields `RentObservation` instances.

    Implement `collect()` to yield observations one at a time; the pipeline
    will materialise them into a DataFrame.
    """

    name: str = "base"

    def __init__(self, *, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.logger = logging.getLogger(f"rent_collector.{self.name}")

    @abstractmethod
    def collect(self) -> Iterator[RentObservation]:
        """Yield rent observations from this source."""
        ...

    def probe(self) -> dict[str, object]:
        """
        Check connectivity and return a status dict.

        Default implementation: call collect(), take the first observation,
        and report success.  Override for faster probing.
        """
        try:
            first = next(iter(self.collect()))
            return {"ok": True, "sample": first.model_dump()}
        except StopIteration:
            return {"ok": True, "sample": None, "note": "no data returned"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
