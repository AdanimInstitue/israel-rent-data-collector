# Israel Official / Public Housing Data Sources — Extended Reference

_Original research by ChatGPT; extended by Claude (Adanim project), 2026-04-21._

---

## What this document adds to the original

The original ChatGPT research identified the five-source Tier 1–3 stack.
This extension adds:

1. **CBS REST API** (`api.cbs.gov.il`) — a fully documented, machine-readable JSON/XML API
   that exposes every CBS price series including the rent price index. Not explored in the
   original pass.

2. **Bank of Israel Hedonic Rent Model** — a published regression equation for estimating
   market rent from location + apartment characteristics. Explicitly mentioned as a target
   by Nethanel in the project kickoff conversation.

3. **CBS Locality Registry on data.gov.il** — the official, machine-readable list of all
   Israeli localities with CBS numeric codes. Essential for building the locality crosswalk
   needed to join nadlan, CBS, and welfare-facility data.

4. **CBS Housing Survey (סקר דיור)** — a granular periodic survey with actual rent paid
   by dwelling size and city; more detailed than Table 4.9.

5. **Ministry of Welfare datasets** (`data.gov.il/he/organizations/molsa`) — residential
   care facility datasets that can cross-reference housing costs.

6. **Community-reverse-engineered nadlan.gov.il API** — three GitHub projects that have
   decoded the backend JSON API used by the nadlan.gov.il SPA, giving us reliable
   programmatic access without scraping HTML.

---

## Full source inventory

| # | Source | Owner | What it covers | Geography | Refresh | Access | Relevance | Notes |
|---|--------|-------|---------------|-----------|---------|--------|-----------|-------|
| 1 | **nadlan.gov.il – Rental trends** | MoJ / Tax Authority | Rental-price medians by locality + room group | Locality | Quarterly | Public SPA + undocumented JSON API (community-decoded) | **★★★★★** | Best official source for city-level rent medians |
| 2 | **CBS REST API – Rent price series** | CBS | Machine-readable rent price index, all series including average rent by room group | District / big city | Monthly | `api.cbs.gov.il` (JSON/XML) | **★★★★★** | Fully documented, no scraping; series IDs listed below |
| 3 | **CBS Table 4.9 – Average Monthly Rent** | CBS | Average rent by district, big cities, room-size groups | Districts + big cities | Quarterly | PDF + Excel download | **★★★★** | Best official cross-check; average not median; limited city coverage |
| 4 | **CBS Locality Registry** | CBS / data.gov.il | All Israeli localities with CBS numeric codes and names | National | Annual | `data.gov.il` CKAN API | **★★★★** | Essential for locality-code crosswalk across all sources |
| 5 | **CBS Dwellings by Ownership 2013–2024** | CBS | Rented-dwelling counts and shares by locality | Locality | Annual | Public PDF | **★★★** | Stock context, not price |
| 6 | **Bank of Israel Hedonic Rent Model** | BoI Research Dept | Regression equation for predicted rent from location + rooms | National (with city dummies) | Periodic (academic) | Published paper (PDF) | **★★★★** | Explicitly requested; gives model-based estimate rather than lookup |
| 7 | **CBS Housing Survey (סקר דיור)** | CBS | Actual rent paid, granular by room count and city | City / district | Periodic | CBS website (Excel/PDF) | **★★★** | More granular than 4.9 but less frequent |
| 8 | **data.gov.il – Ministry of Housing** | MoH via data.gov.il | Public housing vacancies, acquisitions, construction, GIS | Varies | Varies | CKAN API | **★★** | Context / supplementary |
| 9 | **Ministry of Welfare datasets** | MOLSA via data.gov.il | Residential care facility data, costs | Facility / locality | Varies | CKAN API | **★★★** | Direct cross-reference for the Adanim use case |
| 10 | **Tax Authority – Sales DB** | Tax Authority | Sales transactions (not rentals) | National | Ongoing | `nadlan.taxes.gov.il` | **★** | Sales only; context |

---

## Source detail

### 1 — nadlan.gov.il rental trends

The Israeli government real-estate portal (`www.nadlan.gov.il`) displays rental statistics
by locality. The site is a React SPA whose data is fetched from a backend JSON API.

**Community-decoded API endpoints (three GitHub projects):**
- `github.com/jmpfar/gov-nadlan-fetcher`
- `github.com/danielbraun/nadlan.gov.il`
- `github.com/bareini/Nadlan`
- `github.com/nitzpo/nadlan-mcp`

**Likely API patterns** (to be probed by executor):
```
# Rental statistics for a locality
GET https://www.nadlan.gov.il/api/getRentsBySettlement?id={locality_code}
GET https://www.nadlan.gov.il/api/settlement/{locality_code}/rent
POST https://www.nadlan.gov.il/api/RentAnalysis/GetRentBySettlement
```

**Locality IDs** are CBS numeric locality codes. Examples from known URLs:
- `id=3000` → Jerusalem (ירושלים)
- `id=9000` → Beer Sheva (באר שבע)
- `id=6100` → Bat Yam (בת ים)
- `id=6200` → Bnei Brak (בני ברק)
- `id=1247` → small locality (unknown; needs crosswalk)

The full locality code list is available from the CBS Locality Registry (source #4).

**Terms of use:** Government data, public website. No documented explicit prohibition on
programmatic access; polite rate-limiting (≥1 s between requests) is advisable.

---

### 2 — CBS REST API

The CBS exposes all its price and index series via a documented REST API.

**Base URL:** `https://api.cbs.gov.il`

**Key endpoints:**
```
# Catalog of all series (XML)
GET /Index/Catalog/Catalog?lang=en

# All prices in a chapter
GET /index/data/price_all?lang=en&chapter={chapter}&format=json

# Single series
GET /index/data/price?id={series_id}&lang=en&format=json

# Series with date range
GET /index/data/price?id={series_id}&lang=en&format=json&startperiod=2024-01&endperiod=2024-12
```

**Relevant series IDs for rent:**

| Series ID | Description |
|-----------|-------------|
| `150210`  | Housing services group index |
| `150230`  | Actual rental prices index |
| `150220`  | Imputed rental prices |
| `40010`   | Average Housing Indices and Prices (parent group) |
| TBD       | Average rent by room group + city (Table 4.9 equivalent) |

> **Action for executor:** Run `GET /Index/Catalog/Catalog?lang=en` and search the XML for
> series whose names contain "rent" / "שכר" / "דיור" to find the exact Table-4.9 series ID.

**Documentation:** `https://www.cbs.gov.il/en/Pages/Api-interface.aspx`

---

### 3 — CBS Table 4.9 (PDF/Excel)

Published as part of the monthly Consumer Price Statistics publication.

**PDF URL pattern:**
```
https://www.cbs.gov.il/he/publications/Madad/DocLib/{year}/price{month:02d}{letter}/a4_9_e.pdf
```
Example (recent): `https://www.cbs.gov.il/he/publications/Madad/DocLib/2025/price09a/a4_9_e.pdf`

**Excel URL pattern:**
```
https://www.cbs.gov.il/he/publications/Madad/DocLib/{year}/price{month:02d}{letter}/a4_9_e.xlsx
```

Contains: city name, room-count group (2, 2.5, 3, 3.5, 4, 4.5, 5+), average monthly rent (NIS),
standard error, number of observations.

---

### 4 — CBS Locality Registry

**data.gov.il resource ID:** `5c78e9fa-c2e2-4771-93ff-7f400a12f7ba`

**CKAN API call:**
```
GET https://data.gov.il/api/3/action/datastore_search?resource_id=5c78e9fa-c2e2-4771-93ff-7f400a12f7ba&limit=32000
```

Returns: locality name (Hebrew + transliterated), CBS locality code (4-digit), district,
sub-district, population. This is the authoritative crosswalk table.

---

### 5 — Bank of Israel Hedonic Rent Model

**Paper:** "The Changes in Rent in Israel During the Years 2008–2015"
(BoI Research Dept, likely Ribon / Feldman)

**URL:** `https://www.boi.org.il/media/yulnw1sl/part-3n.pdf`

The paper uses a hedonic regression of the form:

```
log(rent_it) = β_0 + β_city * city_dummies + β_rooms * rooms + β_area * floor_area
             + β_floor * floor_number + β_age * building_age + β_t * time_dummies + ε_it
```

For our use case (predicting rent from city + room count), we need:
- `β_city` coefficients (city fixed effects relative to a reference city)
- `β_rooms` coefficient

> **Action for executor:** Download the PDF, extract the regression table, and hard-code
> the coefficients into `collectors/boi_hedonic.py`. The base city in BoI papers is
> typically Tel Aviv or a national average.

**Also relevant:** BoI Research Paper series on housing —
`https://www.boi.org.il/research-and-publications/research-papers/`

---

### 6 — data.gov.il locality registry (CKAN)

**Portal:** `https://data.gov.il/api/3/action/`

**Key dataset URLs:**

```
# Search for rent-related datasets
https://data.gov.il/api/3/action/package_search?q=שכר+דירה&rows=20

# Ministry of Housing datasets
https://data.gov.il/api/3/action/organization_show?id=ministry_of_housing&include_datasets=true

# Ministry of Welfare datasets
https://data.gov.il/api/3/action/organization_show?id=molsa&include_datasets=true
```

---

## Recommended build order

1. **Locality crosswalk** — fetch CBS registry from data.gov.il → get full list of `(locality_code, city_name_he, city_name_en)` for all ~1,200 localities
2. **nadlan.gov.il** — probe API endpoints, collect `(locality_code, room_group, median_rent, quarter)` for all localities where data exists
3. **CBS API** — validate against nadlan; fill gaps for big cities using series from `api.cbs.gov.il`
4. **CBS Table 4.9** — download + parse PDF/Excel as a second validation layer
5. **BoI hedonic** — download paper, extract coefficients, implement `predict_rent(city_code, n_rooms)` as a model-based fallback for localities with no nadlan data
6. **Merge + output** — produce `rent_benchmarks.csv` with columns:
   `locality_code, locality_name_he, locality_name_en, room_group, median_rent_nis, avg_rent_nis, source, quarter, year`

---

## Locality crosswalk strategy

The three main data systems use different identifiers:

| System | ID type | Example |
|--------|---------|---------|
| CBS | 4-digit numeric | `5000` = Tel Aviv |
| nadlan.gov.il | same CBS code used as URL `id` param | `?id=5000` |
| data.gov.il | CBS code embedded in metadata | `settlement_code: "5000"` |
| Care facility spreadsheet | Hebrew city name string | `"תל אביב-יפו"` |

The CBS Locality Registry (source #4) is the **canonical bridge** across all four.

---

## Gaps that remain

- **Live listing count** (how many apartments currently for rent in city X, room bucket Y):
  not available from any official source. Not needed for normative rent estimation.
- **Localities with no nadlan data** (~800 small settlements): use BoI hedonic model or
  district-level CBS average as fallback.
- **Sub-city granularity** (neighborhood level): not available from official sources
  without scraping Madlan/Yad2.
