"""
Abstract base class for public-safe reference-data collectors.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Base interface for collectors that probe or stream public-safe records."""

    name: str = "base"

    def __init__(self, *, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.logger = logging.getLogger(f"rent_collector.{self.name}")

    @abstractmethod
    def collect(self) -> Iterator[object]:
        """Yield source-native records."""
        ...

    def probe(self) -> dict[str, object]:
        """
        Check connectivity and return a status dict.

        Default implementation: call collect(), take the first returned item,
        and report success. Override for faster probing.
        """
        try:
            first = next(iter(self.collect()))
            sample = first.model_dump() if hasattr(first, "model_dump") else first
            return {"ok": True, "sample": sample}
        except StopIteration:
            return {"ok": True, "sample": None, "note": "no data returned"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
