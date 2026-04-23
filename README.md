# israel-nadlan-data-collector

Public-safe collector for Israeli locality reference data.

This repository is intentionally limited to Category-1 public-safe source handling. In milestone 1 it collects and normalizes the data.gov.il / CBS locality registry, writes a reference-data bundle with provenance metadata, and avoids any coupling to non-public systems or repositories.

## Supported Source

| `source_id` | Publisher | Public role |
| --- | --- | --- |
| `data_gov_il_locality_registry` | data.gov.il / CBS | locality metadata and crosswalk support |

Source metadata, terms posture, and attribution rules are documented in [docs/source_policy.md](docs/source_policy.md).

## What The Collector Produces

`build-public-bundle` writes a bundle under `data/public_bundle/` containing:

- `locality_crosswalk.csv`
- `source_inventory.csv`
- `manifest.json`

The repository is self-contained and does not require a sibling checkout to run.

## Install

```bash
python -m pip install -e ".[dev]"
```

Requires Python 3.11 or newer.

## CLI

```bash
indc --help
indc --probe
indc --validate
indc sources list
indc build-public-bundle
indc validate-public-bundle
indc write-manifest
```

The legacy `rent-collector` and `rent-collect` entry points remain available as aliases.

## Repository Layout

```text
src/rent_collector/
  cli.py
  source_registry.py
  provenance.py
  public_bundle.py
  pipeline.py
  collectors/data_gov_il.py
  utils/
configs/
  pipelines/public_release.yaml
  sources/data_gov_il_locality_registry.yaml
docs/
tests/
```

## Public Scope

- Category-1 public-safe source handling only.
- Geography and reference metadata only.
- Rights-aware source metadata and provenance records.
- No references to non-public systems, private repositories, or internal-only source families.

## Development

```bash
pytest
ruff check src tests
mypy src
```

## Workflow Note

`pr-agent-context` is intentionally referenced as floating `@v4` in this repository's GitHub Actions workflows. Do not pin it to a SHA or exact point release.
