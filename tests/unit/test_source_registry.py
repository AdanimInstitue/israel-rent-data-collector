from __future__ import annotations

import pytest

from rent_collector.source_registry import get_source, list_sources


def test_source_registry_contains_only_public_safe_locality_registry() -> None:
    sources = list_sources()
    assert [source.source_id for source in sources] == ["data_gov_il_locality_registry"]
    assert sources[0].source_class == "public_safe"


def test_get_source_raises_for_unknown_source() -> None:
    with pytest.raises(KeyError):
        get_source("unknown_source")
