# Output Datasets Schema

The collector produces two CSV files, written to `data/output/` inside `israel-rent-data-collector` and published to the root of the `israel-rent-data` repo.

---

## 1. `rent_benchmarks.csv`

**Purpose:** One row per `(locality, room group, time period)` with the best available official monthly rent estimate in NIS and full provenance information.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `locality_code` | string | No | CBS locality code, stored as a string. Numeric locality codes may be 1–4 digits (e.g. `"70"` for Ashdod, `"5000"` for Tel Aviv). District aggregates use string IDs such as `"DIST_JER"` and `"DIST_TA"`. |
| `locality_name_he` | string | No | Locality name in Hebrew (from CBS registry or source table). |
| `locality_name_en` | string | No | Locality name in English. |
| `room_group` | string | No | Room-size bucket (see Room Groups below). One of: `"1.0"`, `"1.5"`, `"2.0"`, `"2.5"`, `"3.0"`, `"3.5"`, `"4.0"`, `"4.5"`, `"5.0"`, `"5+"`. |
| `avg_rent_nis` | float | Yes | Source-reported **average** monthly rent in NIS. Null if only median is available. |
| `median_rent_nis` | float | Yes | Source-reported **median** monthly rent in NIS. Null if only average is available. |
| `rent_nis` | float | No | Best single rent estimate used downstream. Set to `median_rent_nis` if available, else `avg_rent_nis`, else the model-predicted value. |
| `source` | string | No | Data source identifier. One of: `"nadlan.gov.il"`, `"cbs_table49"`, `"cbs_api"`, `"boi_hedonic"`. |
| `year` | integer | No | Reference year (e.g. `2025`). |
| `quarter` | integer | Yes | Reference quarter (1–4). Null for annual or model-estimated values. |
| `observations_count` | integer | Yes | Number of rent transactions / observations underlying the estimate when the source exposes it. |
| `notes` | string | Yes | Free-text flags, e.g. `"placeholder-coefficients"` for unvalidated BoI hedonic estimates, or `"fallback-seed"` when the locality came from the seed CSV rather than the live registry. |

### Room Groups

The `room_group` column uses Israeli room count notation:

| Value | Meaning |
|---|---|
| `"1.0"` | 1 room |
| `"1.5"` | 1.5 rooms |
| `"2.0"` | 2 rooms |
| `"2.5"` | 2.5 rooms |
| `"3.0"` | 3 rooms (most common group home size) |
| `"3.5"` | 3.5 rooms |
| `"4.0"` | 4 rooms |
| `"4.5"` | 4.5 rooms |
| `"5.0"` | 5 rooms |
| `"5+"` | 5 or more rooms |

### Source Values

| `source` | Priority | Description |
|---|---|---|
| `nadlan.gov.il` | 1 (highest) | Rent from the Tax Authority real-estate portal. The live settlement JSON currently exposes an average field (`lastYearAvgPrice`), so `avg_rent_nis` is populated and `median_rent_nis` is left null for that shape. |
| `cbs_table49` | 2 | Average rent from CBS Table 4.9 (monthly CPI publication), city or district level |
| `cbs_api` | 3 | Rent price series from CBS REST API at api.cbs.gov.il |
| `boi_hedonic` | 4 (fallback) | Predicted rent from Bank of Israel hedonic regression, calibrated to 2025 prices |

### Deduplication Rules

When multiple sources cover the same `(locality_code, room_group)` cell, only the highest-priority source's row is kept. Within a single source, the row with the most recent `(year, quarter)` is kept.

### Example Rows

```csv
locality_code,locality_name_he,locality_name_en,room_group,median_rent_nis,avg_rent_nis,rent_nis,source,quarter,year,observations_count,notes
5000,תל אביב-יפו,TEL AVIV - YAFO,3.0,,7999.0,7999.0,nadlan.gov.il,1,2025,,nadlan settlement rent page JSON
3000,ירושלים,JERUSALEM,3.0,,5800.0,5800.0,cbs_table49,1,2025,,
9200,אשדוד,Ashdod,3.0,,,4200.0,boi_hedonic,,2025,,placeholder-coefficients
```

---

## 2. `locality_crosswalk.csv`

**Purpose:** Reference table mapping CBS locality codes to Hebrew and English names, districts, and population estimates. Used by downstream consumers who need to join `rent_benchmarks.csv` with other data by locality code or name.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `locality_code` | string | No | CBS locality code (primary key), stored as a string. Numeric locality codes may be 1–4 digits. |
| `locality_name_he` | string | No | Official Hebrew name as published by CBS. |
| `locality_name_en` | string | No | English transliteration (CBS or COGAT standard where available). |
| `district_he` | string | Yes | District name in Hebrew (מחוז). One of: ירושלים, תל אביב, חיפה, מרכז, צפון, דרום, יהודה ושומרון. |
| `district_en` | string | Yes | District name in English. |
| `population_approx` | integer | Yes | Approximate population (most recent available CBS figure). May be null for very small localities or those added from the seed CSV. |
| `source` | string | No | How this record was obtained: `"data.gov.il"` (live fetch) or `"seed_csv"` (bundled fallback). |

### Example Rows

```csv
locality_code,locality_name_he,locality_name_en,district_he,district_en,population_approx,source
5000,תל אביב-יפו,Tel Aviv-Yafo,תל אביב,Tel Aviv,460613,data.gov.il
3000,ירושלים,Jerusalem,ירושלים,Jerusalem,971800,data.gov.il
4000,חיפה,Haifa,חיפה,Haifa,285316,data.gov.il
9000,באר שבע,Beer Sheva,דרום,South,213895,seed_csv
```

---

## File Encoding and Format

Both files use:
- **Encoding:** UTF-8 with BOM (`utf-8-sig`) to ensure correct Hebrew display in Excel
- **Delimiter:** comma (`,`)
- **Line endings:** LF (`\n`)
- **Header row:** yes (first row)
- **Quoting:** only when required (fields containing commas or quotes)
- **Null representation:** empty string (not `NULL`, not `NaN`, not `None`)

## Versioning

The `israel-rent-data` repo uses Git for version history. Each commit represents one collection run. The commit message should follow the pattern:

```
data: collection run {YYYY-MM-DD}

Sources used: nadlan, cbs_table49, boi_hedonic
Localities covered: {N}
Total annual normative rent: {total_NIS:,.0f} NIS
Validation: PASS
```

No explicit version column is included in the CSVs themselves; use `git log` and `git blame` for lineage.
