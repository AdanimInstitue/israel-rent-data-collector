# System Architecture

## Repository Layout

```
adanim_normative_rent/
├── israel-rent-data-collector/     # This repo — collection tool
│   ├── src/rent_collector/
│   │   ├── collectors/             # One module per data source
│   │   │   ├── base.py             # BaseCollector ABC
│   │   │   ├── nadlan.py           # nadlan.gov.il SPA API
│   │   │   ├── cbs_api.py          # CBS REST API (api.cbs.gov.il)
│   │   │   ├── cbs_table49.py      # CBS Table 4.9 Excel/PDF
│   │   │   ├── boi_hedonic.py      # BoI hedonic fallback model
│   │   │   └── data_gov_il.py      # data.gov.il CKAN API
│   │   ├── utils/
│   │   │   ├── http_client.py      # Rate-limited HTTP session
│   │   │   └── locality_crosswalk.py  # CBS code ↔ name lookup
│   │   ├── models.py               # Pydantic data models
│   │   ├── config.py               # All configuration constants
│   │   ├── pipeline.py             # Orchestrator + merge logic
│   │   └── cli.py                  # Click CLI entry point
│   ├── scripts/
│   │   └── collect.py              # Convenience runner
│   ├── data/
│   │   └── locality_codes_seed.csv # ~50 major localities (fallback)
│   ├── docs/                       # Documentation (you are here)
│   └── pyproject.toml
│
└── israel-rent-data/               # Publication repo
    ├── rent_benchmarks.csv         # Primary output
    ├── locality_crosswalk.csv      # Supporting crosswalk
    └── README.md
```

## Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    CLI / scripts/collect.py                  │
│   --source nadlan|cbs-table49|cbs-api|boi-hedonic|all        │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                      pipeline.run_pipeline()                 │
│  1. Instantiate collectors for requested sources             │
│  2. Call collector.collect() → Iterator[RentObservation]     │
│  3. Merge observations (source priority deduplication)       │
│  4. Validate total rent ≥ baseline                           │
│  5. Write output CSVs                                        │
└──────┬──────────────────────────────────────────────────────┘
       │
       ├─── NadlanCollector           (priority 1)
       │      └── proxy-nadlan.taxes.gov.il JSON API
       │
       ├─── CBSTable49Collector       (priority 2)
       │      └── CBS CPI publication Excel/PDF download
       │
       ├─── CBSApiCollector           (priority 3)
       │      └── api.cbs.gov.il REST API
       │
       └─── BoIHedonicCollector       (priority 4 — fallback)
              └── Formula: exp(α + β·rooms + γ·city)
```

## Collectors

### BaseCollector

All collectors extend `BaseCollector` and implement two methods:

- `collect() → Iterator[RentObservation]` — yield observations
- `probe() → dict` — test connectivity without full collection

The `dry_run=True` flag causes `collect()` to log what it *would* do and return immediately, with no network requests.

### NadlanCollector (`nadlan.py`)

The primary source. `nadlan.gov.il` is a React SPA backed by an undocumented JSON API. The SPA fetches data through a proxy host at `proxy-nadlan.taxes.gov.il`.

**Endpoint discovery:** The collector tries a list of candidate endpoint patterns (configured in `config.NADLAN_RENT_ENDPOINTS`) in order. Patterns prefixed with `PROXY:` use `proxy-nadlan.taxes.gov.il` as the base URL; others use `nadlan.gov.il` or `data.nadlan.gov.il`. The first pattern that returns HTTP 200 with parseable JSON is used for all subsequent requests.

**Response shapes handled:**
- Shape A: `[{"rooms": 3, "medianRent": 4200, ...}, ...]` (flat list)
- Shape B: `{"data": [...], "total": ...}` (paginated)
- Shape C: `{"rentByRooms": {"3": {"median": 4200}}}` (keyed by room count)
- Fallback: HTML page with embedded Next.js `__NEXT_DATA__` JSON blob

### CBSTable49Collector (`cbs_table49.py`)

Downloads the monthly CPI Excel file from:
```
https://www.cbs.gov.il/he/publications/Madad/DocLib/{year}/price{month:02d}{letter}/a4_9_e.xlsx
```

If the Excel download fails (HTTP non-200), falls back to the corresponding PDF at the same path with `a4_9_e.pdf`.

**Parsing:** `_latest_price_column()` scans the header rows for the rightmost populated "Average" column and extracts `(year, quarter)`. `_extract_table49_entities()` iterates rows, tracking the current city/district entity name and accumulating (entity, room_group, value) tuples when room-range rows (`1-2`, `2.5-3`, `3.5-4`, `4.5-6`) are encountered.

### CBSApiCollector (`cbs_api.py`)

Queries the CBS REST API at `api.cbs.gov.il`. The `scan_catalog()` method fetches the series catalog and filters for rent-related series, printing a table for operator inspection. As of 2026, the public index catalog does not expose a city-by-room average rent series equivalent to Table 4.9; the `CBS_RENT_SERIES` dict in `config.py` is kept empty until a verified series ID is found.

### BoIHedonicCollector (`boi_hedonic.py`)

Fallback-only estimator based on the Bank of Israel hedonic regression (2008–2015 study, Appendix B Table 8). The model is:

```
log(rent) = 6.8650 + 0.1574 × rooms + γ_city
rent_2025 = exp(log_rent) × calibration_factor × RENT_INFLATION_2015_TO_2025
```

where `calibration_factor` scales the absolute level so that the Tel Aviv 3-room prediction matches the CBS reference figure (`TEL_AVIV_3ROOM_REFERENCE_NIS = 7200`). City effects `γ_city` for 17 cities are stored as a dict; for unknown localities, the mean city effect across all cities is used.

`only_for_missing_localities=True` (default) causes the collector to skip any locality code already present in the `known_localities` set, so it functions purely as gap-filler.

## Merge and Deduplication

`pipeline._merge_observations()` deduplicates by `(locality_code, room_group)`. When multiple sources provide a value for the same cell, the one from the highest-priority source is kept:

```
SOURCE_PRIORITY = [DataSource.NADLAN, DataSource.CBS_TABLE49, DataSource.CBS_API, DataSource.BOI_HEDONIC]
```

Within a single source, the most recent quarter's value is preferred.

## HTTP Client

`utils/http_client.py` provides a `RateLimitedSession` wrapping `requests.Session` with:

- **Per-host rate limiting:** 1.2 s delay between requests to the same host
- **Tenacity retry:** 4 attempts, exponential backoff starting at 2 s, retries on 5xx / connection errors / timeouts
- **User-Agent:** identifies requests as `AdanimInstituteResearch/0.1`
- **Timeout:** 30 s per request

## Locality Crosswalk

`utils/locality_crosswalk.py` maintains a `LocalityCrosswalk` singleton. On first use it fetches the full CBS locality list from `data.gov.il` (CKAN resource `5c78e9fa-c2e2-4771-93ff-7f400a12f7ba`). If that request fails, it loads `data/locality_codes_seed.csv` (~50 major cities). Lookups are by CBS code (`by_code()`) or by a normalised locality name (`by_name()`, with case/spacing/hyphen normalisation).

## Configuration

All tuneable values live in `src/rent_collector/config.py`. Sensitive or environment-specific values can be overridden via a `.env` file (loaded by `python-dotenv`). Key settings:

| Variable | Default | Notes |
|---|---|---|
| `CBS_TABLE49_LATEST_YEAR` | 2025 | Year of the current CPI publication |
| `CBS_TABLE49_LATEST_MONTH` | 9 | Month (01–12) |
| `CBS_TABLE49_LATEST_LETTER` | `a` | Letter suffix in CBS URL |
| `REQUEST_DELAY_SECONDS` | 1.2 | Per-host politeness delay |
| `MAX_RETRIES` | 4 | Tenacity retry attempts |
| `NADLAN_TARGET_YEAR` | 2025 | Target collection year |

## Publishing Pipeline

After a successful validated collection run, the output CSVs are committed to `israel-rent-data`:

```
israel-rent-data-collector/
  └── data/output/rent_benchmarks.csv
  └── data/output/locality_crosswalk.csv
          │
          │  copy + git commit
          ▼
israel-rent-data/
  ├── rent_benchmarks.csv
  └── locality_crosswalk.csv
```

The publishing step is intended to be performed by the Codex executor after each validated run (see `CODEX_HANDOFF_UPDATE.md`). The `israel-rent-data` repo is a plain CSV store — no processing code lives there.

## Environment Setup

```bash
cd israel-rent-data-collector
pip install -e ".[dev]"          # installs all deps including dev extras
python scripts/collect.py --help  # verify CLI loads
python scripts/collect.py --probe # test all endpoint connectivity
```
