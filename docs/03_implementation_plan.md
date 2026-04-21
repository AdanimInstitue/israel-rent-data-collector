# Implementation Plan

This document describes the phased plan for completing, validating, and operationalising the normative rent data pipeline. It is written for both human contributors and Codex-style AI executors.

---

## Phase 0: Environment and Smoke Test (completed)

- [x] Python package scaffold created (`pyproject.toml`, `src/rent_collector/`)
- [x] All collectors implemented with stubs where live API shape is unknown
- [x] CLI entry point works (`python scripts/collect.py --help`)
- [x] BoI hedonic model returns sensible values (`predict("5000", R3_0) == 7200.0`)
- [x] Locality seed CSV present with ~50 major cities

---

## Phase 1: API Discovery and Endpoint Validation

### 1.1 — Probe all endpoints

```bash
python scripts/collect.py --probe
```

This calls `collector.probe()` for every source and prints a JSON summary. Expected output includes:
- `nadlan`: `ok: true, endpoint: <pattern>, format: <shape>`
- `cbs_table49`: `ok: true, format: excel, url: <url>`
- `cbs_api`: `ok: true, status_code: 200`
- `boi_hedonic`: `ok: true, ta_3room_predicted_nis: 7200.0`

**Action if nadlan probe fails:** Try running the endpoint discovery manually against a known locality code (e.g., Tel Aviv = 5000):

```bash
curl "https://proxy-nadlan.taxes.gov.il/api/getRentsBySettlement?settlementCode=5000"
```

If all patterns fail, inspect network traffic on `nadlan.gov.il` in a browser (DevTools → Network → XHR) to find the current API endpoint and update `config.NADLAN_RENT_ENDPOINTS`.

**Action if CBS Table 4.9 probe fails:** The URL template uses `{year}/{month:02d}{letter}`. Verify the current release by browsing:
```
https://www.cbs.gov.il/en/subjects/Pages/Average-Monthly-Prices-of-Rent.aspx
```
Update `CBS_TABLE49_LATEST_YEAR`, `CBS_TABLE49_LATEST_MONTH`, and `CBS_TABLE49_LATEST_LETTER` in `config.py` or `.env`.

### 1.2 — Discover CBS API series

```bash
python scripts/collect.py --source cbs-api --scan-catalog
```

This prints all rent-related series found in the CBS catalog. If a series matching Table 4.9 (city × room group average rent) is found, record its ID in `config.CBS_RENT_SERIES`. As of 2026-04, the public catalog does not appear to expose this data; the source will yield zero observations until a confirmed ID is found.

### 1.3 — Discover nadlan response shape

Fetch one locality (e.g., Tel Aviv) and print the raw JSON:

```bash
python -c "
from rent_collector.collectors.nadlan import NadlanCollector
import json
c = NadlanCollector()
raw = c._raw_fetch('5000')
print(json.dumps(raw, indent=2, ensure_ascii=False)[:2000])
"
```

Identify which response shape (A, B, or C) is returned and verify `_parse_response()` maps it correctly. Update the parser if the actual shape differs.

---

## Phase 2: Full Data Collection

### 2.1 — Dry run

```bash
python scripts/collect.py --source all --dry-run
```

Confirms all collectors initialise without error, logs what each would fetch.

### 2.2 — Nadlan collection (primary source)

```bash
python scripts/collect.py --source nadlan --output data/output/rent_benchmarks_nadlan.csv
```

Iterates all locality codes in the crosswalk. Expect: ~1,200 localities × up to 8 room groups = up to ~9,600 rows. May take 20–40 minutes due to rate limiting (1.2 s/request).

**Quality checks:**
- Row count > 5,000 (otherwise too many localities had no data)
- Tel Aviv 3-room `rent_nis` in range 6,000–9,000 NIS
- Jerusalem 3-room `rent_nis` in range 4,500–7,500 NIS
- No locality appears with `rent_nis < 500` or `rent_nis > 20,000`

### 2.3 — CBS Table 4.9 collection (big cities / districts)

```bash
python scripts/collect.py --source cbs-table49 --output data/output/rent_benchmarks_cbs49.csv
```

Expect: ~30 cities + 6 districts × 4 room groups = ~144 rows.

### 2.4 — Full merged pipeline

```bash
python scripts/collect.py --source all --output data/output/rent_benchmarks.csv --validate
```

Merges all sources using priority order. Validates that total annual normative rent ≥ 131M NIS. If validation fails, investigate which localities are missing.

---

## Phase 3: Validation and Quality Assurance

### 3.1 — Sanity checks

After a full run, perform the following checks:

**Coverage check:** Every locality in the facility registry should have at least one row in `rent_benchmarks.csv`. Cross-join with the Ministry of Welfare's facility list and flag any locality codes with no rent estimate.

**Baseline check (automated):**
```bash
python scripts/collect.py --validate --expected-total-2022 131000000
```

The pipeline sums `rent_nis * 4 * num_residents_proxy` across all facilities; if < 131M NIS, the run is flagged.

**Distribution check:** Review the histogram of `rent_nis` by district. Outliers (extremely high or low values) should be investigated — they may indicate parsing errors or mismatched room groups.

**Source coverage check:** Inspect what fraction of localities are covered by each source tier:

```bash
python -c "
import pandas as pd
df = pd.read_csv('data/output/rent_benchmarks.csv')
print(df.groupby('source')['locality_code'].nunique())
"
```

Ideal: most localities covered by `nadlan`; `cbs_table49` adds district-level estimates; `boi_hedonic` used for fewer than 200 localities.

### 3.2 — Manual spot checks

Compare spot samples against known values:
- Tel Aviv 3 rooms: ~7,200 NIS/month (2025)
- Jerusalem 3 rooms: ~5,500–6,000 NIS/month
- Haifa 3 rooms: ~4,500–5,500 NIS/month
- Beer Sheva 3 rooms: ~3,500–4,500 NIS/month

### 3.3 — Notes field review

Rows with non-empty `notes` fields require attention:
- `"placeholder-coefficients"` — BoI hedonic is using unvalidated coefficients; acceptable for initial run but flag for follow-up
- `"fallback-seed"` — locality came from seed CSV; consider fetching live crosswalk
- `"CBS series NNN; room group not in data"` — CBS API returned data without room group breakdown; use with caution

---

## Phase 4: Publishing to `israel-rent-data`

After validation passes:

### 4.1 — Copy output files

```bash
cp data/output/rent_benchmarks.csv ../israel-rent-data/rent_benchmarks.csv
cp data/output/locality_crosswalk.csv ../israel-rent-data/locality_crosswalk.csv
```

### 4.2 — Commit and push

```bash
cd ../israel-rent-data
git add rent_benchmarks.csv locality_crosswalk.csv
git commit -m "data: collection run $(date +%Y-%m-%d)

Sources used: <list sources that yielded data>
Localities covered: <N>
Total annual normative rent: <total> NIS
Validation: PASS"
git push origin main
```

### 4.3 — Tag the release (optional)

For significant runs (e.g., annual update):

```bash
git tag -a "v$(date +%Y.%m)" -m "Annual normative rent update $(date +%Y-%m)"
git push origin --tags
```

---

## Phase 5: Automation (future)

Once the pipeline is stable and validated manually at least once, consider:

- **GitHub Actions workflow** in `israel-rent-data-collector` that runs the collector quarterly and opens a PR in `israel-rent-data` with the new data
- **Validation gate:** PR is auto-blocked if `--validate` exits non-zero
- **Diff summary:** Action posts a comment summarising changes (localities added/removed, average rent change %)

---

## Known Risks and Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| nadlan.gov.il API URL changes | Medium | Multi-pattern endpoint list; probe on each run |
| CBS Table 4.9 URL template changes | Medium | Check CBS rent page for current link before each run |
| CBS API never exposes city/room rent series | High | CBS Table 4.9 is sufficient for major cities; BoI hedonic covers the rest |
| BoI hedonic coefficients become stale | Low | The 2015 model is calibrated against live CBS reference; recalibrate annually |
| data.gov.il registry outage | Low | Seed CSV fallback covers ~50 major localities |
| nadlan.gov.il blocks scraping | Low | Polite rate limiting + research User-Agent; official government data |
