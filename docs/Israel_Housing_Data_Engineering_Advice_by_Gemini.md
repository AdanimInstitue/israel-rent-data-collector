# Technical Spec: Israel Rental Market Data Engineering
## Target: Automated Retrieval & Harmonization for Agentic Workflows

### 1. Endpoint Discovery (Undocumented Proxy APIs)
While official documentation for a REST API is lacking, the following "hidden" infrastructure is used by government front-ends:
* **Proxy Endpoint:** `proxy-nadlan.taxes.gov.il`
* **Data Payload:** Research identifies that `nadlan.gov.il` trend maps fetch JSON objects via XHR.
* **Agent Task:** Configure the agent to intercept/mimic these requests to bypass PDF/HTML extraction for median rent and room-count breakdowns.

### 2. High-Resolution Mapping (Statistical Areas)
To move beyond city-level averages, the pipeline should ingest **Statistical Area (SA)** identifiers:
* **Source:** CBS "Dwellings by Ownership" dataset.
* **Granularity:** Neighborhood-level blocks.
* **Join Logic:** Use SA codes to correlate market prices with socio-economic clusters. This provides higher variance resolution than standard city medians.

### 3. Data Validation & Heuristics
Agent logic must account for the "Official vs. Market" gap:
* **Room Count Logic:** CBS/Tax Authority data uses legal permit definitions. Market listings (Yad2) often count dining/living areas as full rooms.
    * *Heuristic:* Apply a -0.5 room adjustment when comparing market "Asking" counts to official "Stock" counts.
* **The Lag Factor:** Official medians reflect reported contracts with a 3–6 month lag.
    * *Agent Task:* Use the "Ministry of Housing GIS layers" (updated monthly) as a leading indicator for supply-side shifts before they hit the CBS price tables.

### 4. Harmonization Crosswalk
The agent must maintain a "Locality Crosswalk" table to join these disparate IDs:
| Entity | ID Type | Source |
| :--- | :--- | :--- |
| **CBS** | Locality Code (4-digit) | CBS Tables |
| **Nadlan** | Settlement ID | nadlan.gov.il |
| **Tax Authority** | Gush/Helka | Transaction Database |
| **GIS** | Spatial Polygon | data.gov.il |

### 5. Recommended Pipeline Architecture
1. **Ingest:** Pull Median Rent (Nadlan JSON) + Rental Stock (CBS SA-level).
2. **Proxy:** Scrape current "Asking" counts from private boards to estimate real-time "Demand/Inventory."
3. **Harmonize:** Map all data to the 4-digit CBS Locality Code.
4. **Weight:** Adjust "Asking Price" by a -5% to -10% "Negotiation Factor" based on Bank of Israel (BoI) historical benchmarks.
