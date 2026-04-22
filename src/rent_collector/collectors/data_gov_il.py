"""
data.gov.il CKAN API wrapper.

Provides utility functions for fetching datasets from the Israeli government
open-data portal.  Used by:
  - LocalityCrosswalk (locality registry)
  - Pipeline (to discover new rent-related datasets)
  - Supplementary context fetches (Ministry of Housing, Welfare)

CKAN API base: https://data.gov.il/api/3/action/
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from rich.console import Console

from rent_collector.collectors.base import BaseCollector
from rent_collector.config import DATAGOV_API_BASE
from rent_collector.models import RentObservation
from rent_collector.utils.http_client import get_client

logger = logging.getLogger(__name__)
console = Console(stderr=True)


# ---------------------------------------------------------------------------
# Dataset / resource IDs of interest on data.gov.il
# ---------------------------------------------------------------------------

KNOWN_RESOURCES: dict[str, str] = {
    # CBS Locality Registry
    "locality_registry": "5c78e9fa-c2e2-4771-93ff-7f400a12f7ba",
    # Ministry of Housing — public housing vacancies
    "public_housing_vacancies": "c3a68837-9b7a-4ee7-bd92-130678dc8ae3",
    # Ministry of Housing — public housing acquisitions
    "public_housing_acquisitions": "d6d2046b-ccba-4d09-8778-ee9aa57cdf0c",
    # Add more as discovered; see sources.md for data.gov.il dataset list
}

KNOWN_ORGANIZATIONS: dict[str, str] = {
    "ministry_of_housing": "ministry_of_housing",
    "ministry_of_welfare": "molsa",
    "cbs": "cbs",
}


# ---------------------------------------------------------------------------
# Low-level CKAN helpers
# ---------------------------------------------------------------------------


def ckan_datastore_search(
    resource_id: str,
    *,
    limit: int = 32000,
    offset: int = 0,
    filters: dict[str, Any] | None = None,
    q: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch records from a CKAN datastore resource.

    Automatically paginates if there are more records than `limit`.
    """
    client = get_client()
    url = f"{DATAGOV_API_BASE}/datastore_search"

    params: dict[str, Any] = {
        "resource_id": resource_id,
        "limit": limit,
        "offset": offset,
    }
    if filters:
        import json

        params["filters"] = json.dumps(filters)
    if q:
        params["q"] = q

    all_records: list[dict[str, Any]] = []

    while True:
        data = client.get_json(url, params=params)
        if not data.get("success"):
            logger.warning("CKAN request failed for resource %s: %s", resource_id, data)
            break

        result = data["result"]
        records = result.get("records", [])
        all_records.extend(records)

        total = result.get("total", 0)
        if len(all_records) >= total or len(records) == 0:
            break

        params["offset"] = len(all_records)

    return all_records


def ckan_package_search(query: str, *, rows: int = 20) -> list[dict[str, Any]]:
    """Search for datasets on data.gov.il."""
    client = get_client()
    url = f"{DATAGOV_API_BASE}/package_search"
    data = client.get_json(url, params={"q": query, "rows": rows})
    if not data.get("success"):
        return []
    results = data["result"].get("results", [])
    return [pkg for pkg in results if isinstance(pkg, dict)]


def ckan_organization_datasets(org_id: str) -> list[dict[str, Any]]:
    """List all datasets from an organisation."""
    client = get_client()
    url = f"{DATAGOV_API_BASE}/organization_show"
    data = client.get_json(
        url,
        params={"id": org_id, "include_datasets": "true"},
    )
    if not data.get("success"):
        return []
    packages = data["result"].get("packages", [])
    return [pkg for pkg in packages if isinstance(pkg, dict)]


# ---------------------------------------------------------------------------
# Collector: search data.gov.il for rent-related datasets
# ---------------------------------------------------------------------------


class DataGovILCollector(BaseCollector):
    """
    Supplementary collector that searches data.gov.il for rent / housing datasets
    and emits observations where possible.

    Primarily useful for:
      - Discovering new datasets as they are published
      - Fetching CBS rent data published as open datasets (if available)
      - Fetching welfare / housing context data

    This collector does NOT currently yield RentObservation instances directly
    (the CBS and nadlan data is fetched via dedicated collectors).  Its
    `collect()` method returns an empty iterator; use `discover_datasets()`
    for discovery.
    """

    name = "data_gov_il"

    def collect(self) -> Iterator[RentObservation]:
        """No direct observations — data.gov.il is used for locality crosswalk only."""
        return iter([])

    def discover_datasets(self, query: str = "שכר דירה") -> list[dict[str, Any]]:
        """
        Search data.gov.il for datasets matching `query`.
        Prints a summary table and returns the raw result list.
        """
        console.log(f"Searching data.gov.il for: {query!r}")
        results = ckan_package_search(query, rows=30)

        for pkg in results:
            title = pkg.get("title", "")
            org = (pkg.get("organization") or {}).get("name", "")
            resources = pkg.get("resources", [])
            console.print(f"  [bold]{title}[/bold] — org: {org} — {len(resources)} resources")
            for r in resources[:3]:
                console.print(
                    f"    [{r.get('format', '?')}] {r.get('name', '')} | {r.get('url', '')[:70]}"
                )

        return results

    def probe(self) -> dict[str, object]:
        try:
            records = ckan_datastore_search(KNOWN_RESOURCES["locality_registry"], limit=1)
            return {
                "ok": bool(records),
                "sample_keys": list((records[0] if records else {}).keys()),
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
