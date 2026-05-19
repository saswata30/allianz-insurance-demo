# Allianz Insurance Intelligence — Databricks Demo

End-to-end Databricks demo for **Allianz** spanning Personal Lines, Commercial
Lines, and Specialty Insurance. Generates synthetic data, scrapes real-time
weather + catastrophe feeds into a UC volume, ingests them into bronze, runs a
Lakeflow Declarative Pipeline through silver to gold, exposes a Genie Space for
NL Q&A and an AI/BI dashboard. Packaged as a Databricks Asset Bundle.

## Live Resources

| Resource    | URL                                                                                                                                                                  |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Workspace   | <https://fevm-serverless-stable-xhky6g.cloud.databricks.com>                                                                                                         |
| Catalog     | `serverless_stable_xhky6g_catalog`                                                                                                                                   |
| Schemas     | `allianz_bronze` · `allianz_silver` · `allianz_gold`                                                                                                                 |
| Landing volume | `/Volumes/serverless_stable_xhky6g_catalog/allianz_bronze/landing/external/<feed>/`                                                                              |
| DLT pipeline | `Allianz Insurance — Silver/Gold DLT`                                                                                                                              |
| Full refresh job | `Allianz — Full Refresh (data gen → external land → bronze ingest → DLT)`                                                                                      |
| Hourly feeds job | `Allianz — External Feeds (hourly)`                                                                                                                            |
| Dashboard   | [Allianz Insurance Intelligence](https://fevm-serverless-stable-xhky6g.cloud.databricks.com/dashboardsv3/01f152dad34010d48251741cd17e7e33)                            |
| Genie       | [Allianz Insurance Intelligence — Genie](https://fevm-serverless-stable-xhky6g.cloud.databricks.com/genie/rooms/01f152db5ad41c1c91ec9f08ca683bc3)                     |

## Architecture

<p align="center">
  <img src="docs/diagrams/architecture.svg" alt="Allianz Insurance Intelligence — Lakehouse data flow" width="100%">
</p>

Source: [`docs/diagrams/architecture.mmd`](docs/diagrams/architecture.mmd) (Mermaid).

<details>
<summary>ASCII view</summary>

```
Sources                          Bronze landing volume         Bronze tables       Silver tables       Gold marts
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────
Synthetic core systems ──┐                                ┌──► policies_raw      ┌──► dim_policy     ┌──► underwriting_kpis
(policies, claims,       │                                │    claims_raw        │    fact_claim     │    claims_summary
 premiums, customers,    │                                │    premiums_raw      │    fact_premium   │    exposure_accumulation
 agents, geo,            ├──── direct write ──────────────┤    customers_raw     │    dim_customer   │    solvency_capital
 reinsurance, assets)    │                                │    geography_raw     │    dim_geography  │    asset_portfolio_summary
                         │                                │    asset_portfolio_raw│   dim_asset      │    reinsurance_summary
                         │                                │    ...               │    ...            │    event_risk_correlation
                         │                                │                      │                   │    cat_event_pml
Open-Meteo weather ──────┤   ┌────────────────────────┐   │    weather_obs_raw   │    weather_obs   │
NOAA active alerts ──────┼──►│ /Volumes/.../landing/  ├──►│    noaa_alerts_raw   │    noaa_alerts   │    policy_customer_360  ◀── joined view
USGS earthquakes ────────┤   │   external/<feed>/.parq│   │    catastrophe_*_raw │    catastrophe_*  │    claim_360            ◀── joined view
GDACS catastrophes ──────┤   └────────────────────────┘   │    earthquakes_raw   │    earthquake_*   │    loss_ratio_by_segment
ReliefWeb news ──────────┘    (external_feeds.py)          │    news_raw          │    news_events    │    book_health          ◀── P&C KPIs
                              (ingest_external.py merges)  │                      │                   │    peril_loss_summary
                                                           │                      │                   │
                                                           │    pc_policies_raw   │    pc_policies   │
Migrated allianz_pc tables ────────► merge_allianz_pc.py ─►│    pc_claims_raw     │    pc_combined_* │
(10 P&C reference tables)                                  │    pc_invest_*_raw   │    pc_solvency_* │
                                                           │    ...               │    pc_loss_dev   │
                                                           └─────────────────────┘    ...           │
```

</details>

## Schemas

| Schema           | Purpose                                                  | Tables |
| ---------------- | -------------------------------------------------------- | ------ |
| `allianz_bronze` | Raw landing — synthetic + external (post-volume-merge) + migrated P&C raw | 19 |
| `allianz_silver` | Conformed dims/facts, external silver, P&C reference data | 24 |
| `allianz_gold`   | Business marts (KPIs, joined 360 views, segment loss ratio, book health) | 13 |

## Lines of Business

| LOB         | Products                                                                |
| ----------- | ----------------------------------------------------------------------- |
| Personal    | Auto · Home · Life · Renters · Umbrella                                 |
| Commercial  | Property · General Liability · Workers Comp · D&O · Professional Liability |
| Specialty   | Marine · Aviation · Cyber · Environmental Liability · Energy            |

## Reusable SQL — `sql/`

| File                              | Purpose                                                            |
| --------------------------------- | ------------------------------------------------------------------ |
| `01_underwriting_kpis.sql`        | GWP / NWP / loss / expense / combined ratio by LOB & month        |
| `02_book_health.sql`              | Policy count, GWP, frequency per 100, severity, loss ratio        |
| `03_loss_triangle.sql`            | Accident-year × dev-period loss triangle (IBNR, ultimate loss)    |
| `04_solvency_ii.sql`              | SCR breakdown, own funds, solvency ratio, regulatory status        |
| `05_cat_pml_exposure.sql`         | Live catastrophe events × per-state TIV → PML estimate            |
| `06_segment_loss_ratio.sql`       | Worst-performing LOB × product × state × channel × customer_type   |
| `07_claim_360.sql`                | Claim + policy + customer + geography in one row                   |
| `08_reinsurance_recoveries.sql`   | Ceded premium by reinsurer (USD + EUR books)                       |
| `09_combined_ratio_vs_benchmark.sql` | Sub-line combined ratio vs published industry benchmarks         |
| `10_realtime_event_risk.sql`      | NOAA active alerts × exposed TIV                                   |

Verify all SQL queries against the warehouse:

```bash
uv run --no-project scripts/smoke_test_sql.py
```

## Genie — industry-standard P&C content

The Genie space at the link above includes:

- **30 attached tables** spanning core silver dims/facts, all 10 P&C reference
  tables, 2 live event feeds, and all 13 gold marts.
- **11 instructions** covering Lines of Business, Glossary, Industry Benchmarks
  (Combined Ratio · Frequency/Severity · Solvency II · Reinsurance), Table
  Guidance, Joining Tables, Reporting Conventions, Data Freshness, and a
  How-do-I FAQ.
- **29 curated example questions** across underwriting / profitability, claims,
  exposure, solvency, reserving (loss triangles), reinsurance, assets,
  real-time event risk, and customer / channel mix.

## Running Locally

```bash
# 1. Authenticate
databricks auth login --host https://fevm-serverless-stable-xhky6g.cloud.databricks.com \
  --profile fe-vm-fevm-serverless-stable-xhky6g

# 2. Generate synthetic data → allianz_bronze
uv run --no-project --with polars --with numpy --with mimesis \
  --with "databricks-connect>=16.4,<17.0" \
  src/generate_data.py

# 3. Pull external feeds → UC volume
uv run --no-project --with polars --with httpx src/external_feeds.py

# 4. Merge volume files → allianz_bronze.*_raw
uv run --no-project --with "databricks-connect>=16.4,<17.0" src/ingest_external.py
```

## Deploying via DAB

```bash
databricks bundle validate --profile fe-vm-fevm-serverless-stable-xhky6g
databricks bundle deploy   --profile fe-vm-fevm-serverless-stable-xhky6g
databricks bundle run      allianz_full_refresh --profile fe-vm-fevm-serverless-stable-xhky6g

# Build & deploy the dashboard and the Genie space
uv run --no-project --with httpx scripts/build_dashboard.py
uv run --no-project scripts/build_genie.py
```

## Brand Guidelines

| Element       | Value                                |
| ------------- | ------------------------------------ |
| Primary       | `#003781` (Allianz Blue)             |
| Secondary     | `#006192` (Allianz Light Blue)       |
| Accent        | `#96DCFA` Sky · `#FFAB00` Gold       |
| Font          | Allianz Neo (fallback: Inter)        |
| Voice         | Trustworthy, precise, regulator-aware |
