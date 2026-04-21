# Codex Handoff: israel-rent-data-collector

## What this repo does

Collects official Israeli rental-price benchmarks from government sources.
The output is `data/output/rent_benchmarks.csv`: a table of
`(locality_code, room_group) → median/avg rent (NIS)` covering all ~1,200
Israeli localities, drawn from:

1. **nadlan.gov.il** — Israeli government real-estate portal (primary; median rent)
2. **CBS REST API** at `api.cbs.gov.il` (secondary; average rent)
3. **CBS Table 4.9** PDF/Excel from the CPI publication (validation)
4. **Bank of Israel hedonic model** (fallback for localities with no data)

## Your tasks (in order)

### Task 1 — Install and verify the project runs

```bash
cd israel-rent-data-collector
pip install -e ".[dev]"
python scripts/collect.py --help
```

Expected: help text prints with options `--source`, `--dry-run`, `--probe`, etc.

---

### Task 2 — Probe all endpoints

```bash
python scripts/collect.py --probe
```

This will hit each source and report which ones are reachable.
Report what you see (HTTP status codes, response shapes, error messages).

---

### Task 3 — Discover the live nadlan.gov.il rental API endpoint

The `NadlanCollector` tries four candidate endpoint patterns. Run:

```bash
python scripts/collect.py --source nadlan --dry-run --verbose
```

**If none of the four patterns work:**

1. Open https://www.nadlan.gov.il/?id=5000&page=rent&view=settlement_rent in a browser
2. Open DevTools → Network tab → filter by XHR/Fetch
3. Reload the page and find the API call that returns the rental statistics JSON
4. Copy the endpoint URL and update `NADLAN_RENT_ENDPOINTS` in
   `src/rent_collector/config.py`

**If a pattern works but `_parse_response()` fails:**
- Print `resp.json()` for locality 5000 (Tel Aviv)
- Look at the JSON shape (keys, nesting)
- Update `_parse_response()` in `src/rent_collector/collectors/nadlan.py`
  to handle the actual shape

Reference GitHub repos that have previously reverse-engineered the API:
- https://github.com/jmpfar/gov-nadlan-fetcher
- https://github.com/danielbraun/nadlan.gov.il
- https://github.com/bareini/Nadlan
- https://github.com/nitzpo/nadlan-mcp

---

### Task 4 — Find the correct CBS series ID for Table 4.9

Run:
```bash
python scripts/collect.py --source cbs-api --scan-catalog --dry-run
```

This prints all CBS series containing "rent" / "שכר" in their name.
Find the series that represents **average monthly rent by city + room group**
(the Table 4.9 equivalent).

Then add the confirmed series ID to `CBS_RENT_SERIES` in
`src/rent_collector/config.py`. Example:

```python
CBS_RENT_SERIES: dict[str, str] = {
    "150230": "Actual rental prices index",
    "XXXXXX": "Average monthly rent by room group and city",  # ← add this
}
```

Also check: https://www.cbs.gov.il/en/subjects/Pages/Average-Monthly-Prices-of-Rent.aspx

---

### Task 5 — Extract BoI hedonic coefficients

Download the Bank of Israel paper:
```python
from rent_collector.collectors.boi_hedonic import BoIHedonicCollector
content = BoIHedonicCollector.download_paper()
with open("boi_hedonic_paper.pdf", "wb") as f:
    f.write(content)
```

Open the PDF, find the regression table (look for "Table" in appendix sections
or sections titled "Regression" / "Hedonic"). Extract:
- Intercept (α)
- Coefficient on number of rooms (β)
- City fixed effects (γ_city for each city listed)

Then update `_PLACEHOLDER_COEFFICIENTS` in
`src/rent_collector/collectors/boi_hedonic.py` with the real values and set
`COEFFICIENTS_ARE_PLACEHOLDER = False`.

---

### Task 6 — Run the full pipeline

```bash
python scripts/collect.py --validate --expected-total-2022 131000000
```

Expected output:
- `data/output/rent_benchmarks.csv` (main output)
- `data/output/locality_crosswalk.csv` (locality code ↔ name table)
- Console summary table showing rows and median rents by source and room group

**Minimum acceptable coverage:**
- At least 50 localities with nadlan or CBS data
- At least room groups 2, 3, 4, 5+ covered for major cities
- Annual total rent estimate ≥ 131 million NIS (2022 baseline)

---

### Task 7 — Verify output quality

Run a quick sanity check:

```python
import pandas as pd
df = pd.read_csv("data/output/rent_benchmarks.csv")

# Check expected rent ranges for major cities, 3-room apartments
tel_aviv = df[(df.locality_code == "5000") & (df.room_group == "3.0")]
beer_sheva = df[(df.locality_code == "9000") & (df.room_group == "3.0")]

print("Tel Aviv 3-room:", tel_aviv[["rent_nis", "source"]].to_dict("records"))
print("Beer Sheva 3-room:", beer_sheva[["rent_nis", "source"]].to_dict("records"))

# Expected rough ranges (2025):
# Tel Aviv 3-room:  6,500–9,000 NIS/month
# Beer Sheva 3-room: 2,800–4,200 NIS/month
# If values are wildly outside these ranges, something is wrong.
```

---

## Key files

| File | Purpose |
|------|---------|
| `src/rent_collector/config.py` | All configurable settings (endpoints, series IDs, etc.) |
| `src/rent_collector/collectors/nadlan.py` | nadlan.gov.il collector — update endpoint + parser |
| `src/rent_collector/collectors/cbs_api.py` | CBS API collector — update series IDs |
| `src/rent_collector/collectors/boi_hedonic.py` | BoI model — update coefficients |
| `sources.md` | Annotated source inventory with URLs and API docs |

## Expected output schema

```
locality_code    : str  — CBS 4-digit code, e.g. "5000"
locality_name_he : str  — Hebrew name, e.g. "תל אביב-יפו"
locality_name_en : str  — English name, e.g. "Tel Aviv-Yafo"
room_group       : str  — "2.0", "2.5", "3.0", "3.5", "4.0", "4.5", "5.0", "5+"
median_rent_nis  : float|null — from nadlan.gov.il
avg_rent_nis     : float|null — from CBS
rent_nis         : float — best available (median > avg)
source           : str  — "nadlan.gov.il" | "cbs_table49" | "cbs_api" | "boi_hedonic"
quarter          : int  — 1-4
year             : int
```

## What NOT to do

- Do NOT scrape Madlan.co.il or Yad2.co.il (ToS concerns)
- Do NOT use any non-official data source without flagging it
- Do NOT hardcode rent values by hand — everything must come from a documented source
