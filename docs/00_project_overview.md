# Project Overview: Normative Rent for Israel Residential Care Facilities

## Background

The Adanim Institute conducts policy research and analysis related to Israel's social welfare system. One core area of work is auditing the cost structures of **out-of-home residential care facilities** (מסגרות חוץ-ביתיות) — placements in group homes, supported living apartments, and similar residential settings funded through public welfare programs.

A key cost component in these placements is **housing rent**. The Ministry of Welfare reimburses care providers for rent, but the reimbursement rates are set normatively — i.e., based on what market-rate rent *should* cost for a given facility type, not what each provider actually pays. Calculating a defensible, reproducible normative rent figure for every facility is therefore essential for budget oversight, policy advocacy, and provider-side benchmarking.

## The Research Question

> For each residential care facility registered in Israel — given its **city/locality** and **number of residents** (which determines the apartment size) — what is the estimated **market-rate monthly rent** (NIS)?

This normative figure is compared against actual reimbursements to identify over- or under-compensation at scale.

**Key constraint:** only official, government-sourced data may be used. Commercial portals such as Madlan and Yad2 are excluded due to licensing considerations.

## Validation Baseline

An earlier analysis (Nethanel, 2022) established that the total annual normative rent across all registered facilities is approximately **131 million NIS**. All pipeline outputs must be checked against this baseline as a sanity test.

## The Two-Repo Architecture

The project is split across two GitHub repositories under the **AdanimInstitue** organisation:

| Repo | Purpose |
|---|---|
| [`israel-rent-data-collector`](https://github.com/AdanimInstitue/israel-rent-data-collector) | Python tool that collects, merges, and validates rent benchmark data from official sources |
| [`israel-rent-data`](https://github.com/AdanimInstitue/israel-rent-data) | Periodic publication destination — stores the generated CSVs so downstream users/scripts can consume them without running the collector |

The `israel-rent-data` repo is a **data-only** repo. It receives commits from the collector tool when a new collection run produces validated output.

## Data Sources (Priority Order)

The collector queries official sources in the following priority order, using the highest-quality source available for each locality/room-group combination:

| Priority | Source | Coverage | Format |
|---|---|---|---|
| 1 | **nadlan.gov.il** | ~1,200 localities, quarterly rent transactions | JSON API (SPA proxy) |
| 2 | **CBS Table 4.9** | ~30 big cities + 6 districts, quarterly | Excel / PDF download |
| 3 | **CBS REST API** | National + district indices | JSON REST API |
| 4 | **BoI Hedonic Model** | All localities (estimated) | Formula: intercept + β·rooms + γ·city |

For localities covered by nadlan.gov.il, that source is definitive. CBS Table 4.9 fills in gaps for major cities. The Bank of Israel hedonic regression (based on their 2008–2015 study, calibrated to 2025 price levels) provides a last-resort estimate for localities with no transaction data.

## Room Group Convention

Israeli apartment sizes are described in Israeli "rooms" (חדרים), which run from 1 to 5+ in 0.5-room increments. The collector uses a `RoomGroup` enum:

| Enum | Range | Typical facility use |
|---|---|---|
| `R2_0` | 1–2 rooms | Individual placements, small groups |
| `R2_5` | 2.5 rooms | — |
| `R3_0` | 3 rooms | Standard group home (2–4 residents) |
| `R3_5` | 3.5 rooms | — |
| `R4_0` | 4 rooms | Medium group home (4–6 residents) |
| `R4_5` | 4.5 rooms | — |
| `R5_0` | 5 rooms | Large group home |
| `R5_PLUS` | 5+ rooms | Large group home / hostel |

## Locality Identification

All Israeli localities are identified by **CBS locality codes** (מספר ישוב) — 4-digit numeric identifiers published by the Central Bureau of Statistics. These codes are the universal join key across all government datasets.

The collector maintains a `LocalityCrosswalk` that maps CBS codes to Hebrew and English names. The crosswalk is populated at runtime from the CBS locality registry on `data.gov.il` (resource ID `5c78e9fa-c2e2-4771-93ff-7f400a12f7ba`), with a bundled seed CSV as fallback.

## Output Files

The collector produces two CSV files (published to `israel-rent-data`):

- **`rent_benchmarks.csv`** — One row per (locality, room_group, quarter), with average rent in NIS and provenance metadata
- **`locality_crosswalk.csv`** — One row per CBS locality code, with Hebrew/English names and district

See `02_output_datasets_schema.md` for full column-level documentation.

## Key Personnel

- **Shay Palachy** — Head of AI & Data, Adanim Institute; technical lead for this project
- **Nethanel** — Research analyst; provided the original 2022 normative rent estimate and business requirements
