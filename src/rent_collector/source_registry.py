from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class SourceDescriptor:
    source_id: str
    display_name: str
    source_class: str
    homepage_url: str
    terms_url: str | None
    license_url: str | None
    access_method: str
    record_grain: str
    expected_refresh_pattern: str
    citation_text: str
    attribution_required: bool
    redistribution_note: str
    status: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


PUBLIC_SOURCE_REGISTRY: tuple[SourceDescriptor, ...] = (
    SourceDescriptor(
        source_id="data_gov_il_locality_registry",
        display_name="data.gov.il / CBS locality registry",
        source_class="public_safe",
        homepage_url="https://data.gov.il/dataset/citiesandsettelments",
        terms_url=None,
        license_url=None,
        access_method="ckan_api",
        record_grain="locality_reference_record",
        expected_refresh_pattern="periodic_registry_update",
        citation_text="data.gov.il / CBS locality registry.",
        attribution_required=True,
        redistribution_note="Retain source attribution for geography metadata.",
        status="active",
    ),
)


def list_sources() -> list[SourceDescriptor]:
    return list(PUBLIC_SOURCE_REGISTRY)


def get_source(source_id: str) -> SourceDescriptor:
    for source in PUBLIC_SOURCE_REGISTRY:
        if source.source_id == source_id:
            return source
    raise KeyError(source_id)
