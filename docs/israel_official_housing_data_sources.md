# Israel official / public housing data sources for rental-market research

_Last verified: 2026-04-21_

## Purpose

This brief inventories the strongest **official or public Israeli housing-data sources** I found that are relevant to:

1. city-level rental pricing,
2. rental stock / housing stock,
3. housing supply and public-housing context,
4. housing-market comparison data.

It is written so that another AI agent or human researcher can quickly decide which sources to use first.

---

## Executive summary

### Bottom line for the exact target you described

Your target was:

- **per-city count of apartments for rent by category / room-count**, and
- **median asking rent / price per city**.

### What exists officially / publicly

- There **is** an official public source that is very close to the **price** side:
  - **nadlan.gov.il → rental trends by locality**
  - This appears to expose **locality-level rental medians**, including room-count group breakdowns, via a public government website.

- There **are** official sources for **rental stock / rented-dwelling counts**:
  - **CBS: Dwellings by Ownership in Israel, 2013–2024**
  - This gives rented-dwelling counts / shares by locality and, in some cases, by statistical area.

- There **are** official sources for **average** rent levels:
  - **CBS Table 4.9: Average Monthly Prices of Rent**, by district / big city / room group.

- There **are** official sources for broader **housing-market context**:
  - Tax Authority real-estate transactions,
  - Ministry of Housing datasets on public housing, housing programs, GIS layers, and construction progress.

### What I did **not** find officially in this pass

I did **not** find a nationwide official open source for **live listing inventory** equivalent to:

- “how many rental listings are currently advertised in city X, in 2-room / 3-room / 4-room buckets”.

So the official/public ecosystem seems to support:

- **rent levels**,
- **rental stock**,
- **sales transactions**,
- **construction / supply / public housing context**,

but **not** a national official feed of **current asking-rent listings inventory**.

---

## Recommended source stack for your use case

### Tier 1 — use first

1. **nadlan.gov.il – Rental trends by locality**
   - Best source here for **city-level rental medians**.
   - Use when you need: city-by-city rent benchmarks, room-count medians, quarterly trend monitoring.

2. **CBS – Dwellings by Ownership in Israel, 2013–2024**
   - Best source here for **rented-dwelling counts / shares**, by locality and in some cases by statistical area.
   - Use when you need: city rental-stock measures, structural rental-market context, welfare / service-coverage analysis.

3. **CBS – Table 4.9 Average Monthly Prices of Rent**
   - Best official cross-check for **average rent** by room-size bucket and large cities.
   - Use when you need: official rent-level validation, especially for larger cities.

### Tier 2 — strong supplements

4. **Tax Authority real-estate information / transactions**
   - Best for **sales-market context**, not rentals.
   - Use when you need: purchase-price context, housing-market comparison, pressure on ownership vs. rental markets.

5. **Ministry of Housing datasets on data.gov.il**
   - Best for **policy / supply / public-housing context**.
   - Use when you need: public-housing vacancies, acquisitions, construction progress, affordable-housing program activity.

### Tier 3 — context only

6. **GIS layers, housing-program datasets, program-specific data**
   - Useful to enrich local policy analysis, but not a substitute for live rental-listings inventory.

---

## Source inventory matrix

| # | Source | Official owner | What it covers | Geography | Time / refresh | Access mode | Relevance to your target | Main caveats |
|---|---|---|---|---|---|---|---|---|
| 1 | **nadlan.gov.il – Rental trends by locality** | Government real-estate site | Rental-price trends by locality; public pages show **median rent** and room-count breakdowns | Locality / city; sometimes neighborhood context on site | Appears quarterly on public pages | Public website | **High** for rent median estimation | I did not confirm a documented open API in this pass; public-site extraction / structured access still needs terms review |
| 2 | **CBS – Dwellings by Ownership in Israel, 2013–2024** | CBS | Dwelling stock split by ownership / rent / sheltered housing; locality-level rented dwellings; some statistical-area outputs | National, districts, selected localities, some statistical areas | Annual series 2013–2024; published 2025-03-19 | Public PDF / release | **High** for rental-stock counts and city rental-market structure | Not live listings; stock, not current advertised vacancies |
| 3 | **CBS – Table 4.9 Average Monthly Prices of Rent** | CBS | **Average monthly rent** by district, big cities, and room-size groups | Districts + big cities | Quarterly / annual table in monthly price publication | Public PDF table | **Medium–High** for validating city rent levels | Average, not median; limited city coverage (big cities rather than all localities) |
| 4 | **Tax Authority – Real Estate Information Database** | Israel Tax Authority | Sale transactions and property transaction details (rooms, area, year built, estimated value, etc.) | National | Ongoing administrative data | Public government service / public transaction site | **Medium** for broader housing-market context | This is a **sales** source, not rentals; a 2025 State Comptroller report raised quality / completeness concerns |
| 5 | **data.gov.il – Ministry of Housing organization page / housing datasets** | Ministry of Housing via government open-data portal | Aggregates multiple housing datasets in one official portal | Varies by dataset | Varies | Open-data portal | **Medium** as a discovery hub | Not a single dataset; you still need to choose specific resources |
| 6 | **Ministry of Housing – GIS layers** | Ministry of Housing | Geographic layers relevant to housing / programs / urban-renewal / subsidized programs | Geographic layers | Search snippet says **updated monthly** | data.gov.il dataset / GIS resources | **Low–Medium** for direct rent estimation; useful for enrichment | More policy / spatial context than rental-market measurement |
| 7 | **Construction progress reports – dense construction (בנייה רוויה)** | Ministry of Housing | Building / project progress data for residential construction | Project / locality level | Search snippet shows update date 2024-03-01 | data.gov.il dataset | **Medium** for supply-side context | Not rental data; usefulness depends on project fields and coverage |
| 8 | **Public housing vacancies** | Ministry of Housing | Public-housing apartments vacant for >3 months, managed by major public-housing companies | Property / locality | Search snippet shows update date 2026-03-13 | data.gov.il dataset | **Medium** for welfare / housing-services activism | Public housing only; not general market rentals |
| 9 | **Public housing acquisitions** | Ministry of Housing | Apartments purchased into public-housing stock, by year and locality | Locality / year | Search snippet shows update date 2026-04-09 | data.gov.il dataset | **Medium** for public-housing expansion analysis | Public housing only; not general market rentals |
| 10 | **Periodic “Dira BeHana’ah” / subsidized-housing program data** | Ministry of Housing | Lottery / subsidized-sale program data; “apartments for sale without lottery” etc. | Program / project / locality dependent | Periodic; search snippets show frequent updates | data.gov.il dataset | **Low–Medium** for general affordability context | Program-specific; more useful for policy context than rent-market measurement |

---

## Detailed notes by source

## 1) nadlan.gov.il – rental trends by locality

### Why it matters

This is the strongest official/public source I found for the **rent-price** part of your problem.

Public rental-trend pages on nadlan.gov.il display:

- locality name,
- selected quarter,
- locality-level rent figure,
- national comparison,
- room-count breakdowns.

Examples returned by search included:

- Jerusalem,
- Bat Yam,
- Be’er Sheva,
- Harish,
- and other localities.

### Best use

- Build an official benchmark table of **median rent by city**.
- Use as the “official price anchor” in reports.
- Combine with CBS stock data for a city-level rental-market dashboard.

### Main limitation

I did **not** confirm, in this pass, a documented open API or downloadable bulk extract. So from a tooling standpoint it looks like a **public official website**, but not necessarily an explicitly open bulk-data feed.

---

## 2) CBS – Dwellings by Ownership in Israel, 2013–2024

### Why it matters

This is the strongest official/public source I found for the **count / stock** side of rental housing.

The release states that:

- it publishes data on dwellings by **ownership / rent / sheltered housing**,
- the data are processed from the **Dwelling and Building Register**,
- the register is based on **municipal arnona data**,
- outputs include **dwellings and rented dwellings by locality**,
- and also **share of rented dwellings by statistical areas** in municipalities / local councils.

### Best use

- Estimate the **size of the rental stock** by city.
- Measure rental-market dependence of localities.
- Support welfare / social-services research on where renting is concentrated.
- Produce city typologies: high-rental-share vs. low-rental-share municipalities.

### Main limitation

This is **stock**, not **live listing inventory**. It does not tell you how many apartments are currently advertised for rent right now.

---

## 3) CBS – Table 4.9 Average Monthly Prices of Rent

### Why it matters

This is the best official source I found for an easy-to-cite **rent level table** with room-size group breakdowns.

The table includes:

- residential districts,
- big cities,
- room-size groups,
- average rent,
- sampling errors,
- quarterly and annual values.

### Best use

- Validate other price sources.
- Compare city / district rent levels by room groups.
- Use in formal policy writing when an official statistical table is needed.

### Main limitation

- It reports **average** rent, not **median**.
- Coverage is centered on **big cities**, not necessarily all localities.

---

## 4) Tax Authority – Real Estate Information Database

### Why it matters

This is one of the most important official housing-data sources in Israel for **sales transactions**.

Public descriptions indicate it provides access to:

- information on **sales of real-estate rights** in Israel,
- filtering of transactions,
- transaction-related fields such as rooms / area / year built / estimated value.

### Best use

- Compare rent pressure with local purchase prices.
- Build sale-vs-rent context for housing-affordability research.
- Analyze local sale-market activity, especially where rentals and ownership may interact.

### Main limitation

- It is **not a rental-listings source**.
- It is a **sales / transaction** source.
- The **2025 State Comptroller** reported significant quality / completeness issues in the underlying real-estate transactions and prices file. If you use this source heavily, downstream agents should include quality caveats.

---

## 5) data.gov.il – Ministry of Housing open-data resources

### Why it matters

data.gov.il is the official government open-data portal, and the Ministry of Housing organization page acts as a **discovery hub** for housing-related datasets.

### Best use

- Start here when you want **machine-readable** or portal-based official datasets.
- Use this to discover specific resources for public housing, housing programs, construction progress, and GIS layers.

### Main limitation

It is a **portal**, not itself a single harmonized housing dataset.

---

## 6) Ministry of Housing GIS layers

### Why it matters

The housing GIS layers appear to update monthly and can help with:

- mapping projects,
- urban renewal,
- subsidized housing,
- spatial overlays with welfare-service coverage.

### Best use

- Join spatial housing context to welfare / social-service geography.
- Create maps, especially where neighborhood / spatial-service analysis matters.

### Main limitation

This is not a direct source of city-level current rental inventory or market medians.

---

## 7) Construction progress reports

### Why it matters

Construction progress is a useful **supply-side** indicator.

### Best use

- Add housing-supply context to city rental pressure.
- Identify places where lagging construction may interact with rent stress.

### Main limitation

Construction progress does not tell you what is currently listed for rent or what tenants are asking right now.

---

## 8) Public housing vacancies

### Why it matters

This is directly relevant for welfare, housing insecurity, and service-access activism.

### Best use

- Analyze where public-housing stock is sitting vacant.
- Compare vacancy geography with welfare demand and service delivery.

### Main limitation

It covers **public housing only**, not the broader private rental market.

---

## 9) Public housing acquisitions

### Why it matters

This is a strong policy / intervention dataset for tracking whether public-housing inventory is expanding.

### Best use

- Track expansion of public-housing stock over time and by locality.
- Tie housing policy activity to local welfare-system strain.

### Main limitation

Again, this is public housing policy, not current market listings.

---

## 10) Subsidized-housing program data (“Dira BeHana’ah”)

### Why it matters

Useful for affordability and supply-policy context.

### Best use

- Track subsidized housing availability,
- identify local program activity,
- enrich narratives about affordability interventions.

### Main limitation

Program-specific and not a replacement for open rental-market inventory data.

---

## Best no-scrape build path for your use case

If you want a legally cleaner and still useful dataset, I would suggest this stack:

### Core table A — official rent-price benchmark

Use **nadlan.gov.il rental trends by locality** to build:

- `locality`
- `quarter`
- `room_group`
- `official_rent_median_nis`
- `national_comparison`

### Core table B — rental stock context

Use **CBS Dwellings by Ownership** to build:

- `locality`
- `year`
- `rented_dwellings_count`
- `rented_dwellings_share`
- `owned_dwellings_count`
- `total_dwellings`

### Core table C — official average rent cross-check

Use **CBS Table 4.9** to build:

- `city`
- `quarter`
- `room_group`
- `official_avg_rent_nis`
- `sampling_error`

### Context tables

Use data.gov.il / Tax Authority sources to add:

- sales-market context,
- construction / supply context,
- public-housing context,
- geography / spatial overlays.

---

## If you still need “current listing counts by city and room bucket”

That exact variable still appears to be the gap.

A careful downstream agent should distinguish between:

1. **official rental stock** — how many dwellings are rented in the city,
2. **official / public rent levels** — average / median rents,
3. **current advertised inventory** — how many listings are live right now.

The first two seem available from official/public sources.
The third does **not** appear to have an equivalent official nationwide open source in this pass.

---

## Suggested next-step research tasks for another agent

1. Check whether **nadlan.gov.il** exposes a documented or semi-documented structured endpoint for rental-trend pages.
2. Check whether the **CBS rent tables** exist in a more machine-readable form than PDF.
3. Enumerate all **Ministry of Housing** datasets on data.gov.il tagged to:
   - housing,
   - rent,
   - public housing,
   - construction,
   - urban renewal.
4. Verify **reuse / license terms** source by source, especially for nadlan.gov.il pages versus data.gov.il datasets.
5. Build a locality code crosswalk across:
   - CBS locality codes,
   - nadlan locality IDs,
   - Ministry of Housing locality names / codes,
   - Tax Authority / transaction locality identifiers.

---

## Source links

### Core sources

- Government real-estate site homepage:
  - https://www.nadlan.gov.il/

- Example nadlan rental trends pages returned in search:
  - https://www.nadlan.gov.il/?id=6100&page=rent&view=settlement_rent
  - https://www.nadlan.gov.il/?id=3000&page=rent&view=settlement_rent
  - https://www.nadlan.gov.il/?id=6200&page=rent&view=settlement_rent
  - https://www.nadlan.gov.il/?id=9000&page=rent&view=settlement_rent
  - https://www.nadlan.gov.il/?id=1247&page=rent&view=settlement_rent

- CBS – Dwellings by Ownership in Israel, 2013–2024:
  - https://www.cbs.gov.il/he/mediarelease/DocLib/2025/090/04_25_090b.pdf

- CBS – Table 4.9 Average Monthly Prices of Rent:
  - https://www.cbs.gov.il/he/publications/Madad/DocLib/2025/price09a/a4_9_e.pdf

- Tax Authority – real-estate information service:
  - https://www.gov.il/he/service/real_estate_information
  - Tax Authority public transaction site:
    - https://nadlan.taxes.gov.il/svinfonadlan2010/startpageNadlanNewDesign.aspx

### Open-data portal sources

- Government open-data portal:
  - https://data.gov.il/

- Ministry of Housing organization page on data.gov.il:
  - https://data.gov.il/he/organizations/ministry_of_housing

- Housing-tagged datasets on data.gov.il:
  - https://data.gov.il/he/datasets?tags=%D7%93%D7%99%D7%95%D7%A8

- Ministry of Housing GIS layers:
  - https://data.gov.il/he/datasets/ministry_of_housing/gis-_

- Construction progress (dense construction):
  - https://data.gov.il/he/datasets/ministry_of_housing/hitkadmuthabnia
  - https://data.gov.il/he/datasets/ministry_of_housing/hitkadmuthabnia/1ec45809-5927-430a-9b30-77f77f528ce3

- Public housing vacancies:
  - https://data.gov.il/he/datasets/ministry_of_housing/diurtziburi/c3a68837-9b7a-4ee7-bd92-130678dc8ae3

- Public housing acquisitions:
  - https://data.gov.il/he/datasets/ministry_of_housing/diurtziburi/d6d2046b-ccba-4d09-8778-ee9aa57cdf0c

- Periodic subsidized-housing program data (“Dira BeHana’ah”):
  - https://data.gov.il/he/datasets/ministry_of_housing/mechir-lamishtaken
  - https://data.gov.il/he/datasets/ministry_of_housing/mechir-lamishtaken/ea93b3c9-15e2-4b74-a632-097ee53737e4

### Quality / caveat source for Tax Authority data

- State Comptroller abstract on real-estate taxation and real-estate information infrastructure:
  - https://library.mevaker.gov.il/sites/DigitalLibrary/Documents/2025/2025-10/EN/2025.10-201-Nadlan-Taktzir-EN.pdf

---

## Minimal recommendation

If another agent has to act quickly, the most efficient first move is:

1. treat **nadlan.gov.il rental trends** as the best official/public source for **rent medians**,
2. treat **CBS ownership / rental-stock data** as the best official source for **rented-dwelling counts / shares**,
3. treat **CBS Table 4.9** as the best official validation source for **average rents by room group**,
4. use **data.gov.il housing datasets** only as supplementary context,
5. assume that **no official nationwide open source for live listing counts** has yet been identified.

