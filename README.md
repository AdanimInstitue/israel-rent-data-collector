# israel-rent-data-collector

Collects official Israeli rental-price benchmarks from government and public sources,
producing a clean `(city, room_count) → rent_benchmark_NIS` table for use in normative-rent
calculations for out-of-home welfare care facilities.

## Purpose

Part of the Adanim Institute normative-rent project. The output of this tool feeds
Step 2 of the normative-rent pipeline: given a care facility in a known city with a
known number of residents, estimate the market rent it *should* be paying.

## Data sources

See [`docs/sources.md`](docs/sources.md) for the full annotated inventory. In priority order:

| Priority | Source | What we get |
|----------|--------|------------|
| 1 | nadlan.gov.il JSON API | Locality + room-group rent observations, currently average-oriented in the live payload shape |
| 2 | CBS REST API (`api.cbs.gov.il`) | Average rent by district/city + room group |
| 3 | CBS Table 4.9 PDF | Average rent cross-check |
| 4 | BoI hedonic model | Model-based fallback for localities with no data |
| 5 | CBS Locality Registry | Crosswalk: CBS code ↔ city name |

All sources are official Israeli government or public research publications.
No scraping of Madlan, Yad2, or other commercial real-estate portals.

## Output

Running the pipeline produces `data/output/rent_benchmarks.csv`:

```
locality_code, locality_name_he, locality_name_en, room_group,
median_rent_nis, avg_rent_nis, rent_nis, source, quarter, year,
observations_count, notes
```

And `data/output/locality_crosswalk.csv`:
```
locality_code, locality_name_he, locality_name_en, district_he, district_en,
population_approx, source
```

These snippets reflect the current published CSV columns. For the full maintained schema,
see [`docs/02_output_datasets_schema.md`](docs/02_output_datasets_schema.md).

## Installation

```bash
cd israel-rent-data-collector
pip install -e ".[dev]"
```

Requires Python 3.11+.
This installs the Click CLI as `rent-collector` (and the legacy alias `rent-collect`).

## Usage

```bash
# Full pipeline (all sources)
rent-collector

# Legacy alias / direct script entrypoint
rent-collect
python scripts/collect.py

# Single source
rent-collector --source nadlan
rent-collector --source cbs-api
rent-collector --source cbs-table49
rent-collector --source boi-hedonic

# Dry run (probe endpoints, don't save)
rent-collector --dry-run

# Validate output shape and sanity bounds
rent-collector --validate

# Optionally print a 2022 facility-level reference baseline for context
rent-collector --validate --reference-total-2022 131000000
```

## Project structure

```
src/rent_collector/
├── cli.py                   # Click CLI entry point
├── config.py                # Configuration (timeouts, output paths, etc.)
├── models.py                # Pydantic data models
├── pipeline.py              # Orchestrates all collectors
├── collectors/
│   ├── base.py              # Abstract base collector
│   ├── nadlan.py            # nadlan.gov.il rental medians
│   ├── cbs_api.py           # CBS REST API (api.cbs.gov.il)
│   ├── cbs_table49.py       # CBS Table 4.9 PDF/Excel download + parse
│   ├── data_gov_il.py       # data.gov.il CKAN API wrapper
│   ├── boi_hedonic.py       # Bank of Israel hedonic regression model
└── utils/
    ├── http_client.py       # Rate-limited HTTP client with retries
    └── locality_crosswalk.py# Locality code registry and city-name crosswalk
data/
├── locality_codes_seed.csv  # Seed data for ~50 major cities (offline fallback)
└── output/                  # Generated output (gitignored)
```

## Notes on API stability

- **nadlan.gov.il**: The JSON API is not officially documented; endpoints were reverse-
  engineered by community projects. Multiple endpoint patterns are tried in sequence;
  the first one that returns data wins. If all fail, the collector falls back to HTML
  scraping of the public rental-trends pages.
- **CBS API**: Fully documented and stable. Series IDs are listed in `sources.md`.
- **data.gov.il**: Standard CKAN API; stable.

## Development

```bash
# Run tests
pytest

# Lint
ruff check src/
mypy src/
```
