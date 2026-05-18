# Allianz Insurance Intelligence — Databricks Demo

End-to-end Databricks demo for **Allianz** spanning Personal Lines, Commercial Lines,
and Specialty Insurance. Generates synthetic data, ingests external real-time
weather + catastrophe feeds, builds a medallion pipeline with Lakeflow Declarative
Pipelines, exposes a Genie Space for natural-language Q&A, and an AI/BI dashboard.
Packaged as a Databricks Asset Bundle.

## Live Resources

| Resource    | URL                                                                                                                                                                  |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Workspace   | <https://fevm-serverless-stable-xhky6g.cloud.databricks.com>                                                                                                         |
| Catalog     | `serverless_stable_xhky6g_catalog`                                                                                                                                   |
| Schemas     | `allianz_bronze` · `allianz_silver` · `allianz_gold` · `allianz_ext`                                                                                                 |
| DLT pipeline | `Allianz Insurance — Silver/Gold DLT`                                                                                                                                |
| Full refresh job | `Allianz — Full Refresh (data gen → external feeds → DLT)`                                                                                                       |
| Hourly feeds job | `Allianz — External Feeds (hourly)`                                                                                                                              |
| Dashboard   | [Allianz Insurance Intelligence](https://fevm-serverless-stable-xhky6g.cloud.databricks.com/dashboardsv3/01f152dad34010d48251741cd17e7e33)                            |
| Genie       | [Allianz Insurance Intelligence — Genie](https://fevm-serverless-stable-xhky6g.cloud.databricks.com/genie/rooms/01f152db5ad41c1c91ec9f08ca683bc3)                     |

## Architecture

```
Sources                         Bronze              Silver              Gold              Consumption
──────────────────────────────────────────────────────────────────────────────────────────────────────
Synthetic core systems ──┐                                                              ┌─ Genie Space
(policies, claims,       │                                                              │  (NL Q&A)
 premiums, customers,    │                                                              │
 agents, treaties,       ├──► allianz_bronze ──► (DLT) ──► allianz_gold ──► allianz_gold ──►┤
 assets, geography)      │                                                              │
                         │                                                              └─ AI/BI Dashboard
Open-Meteo weather  ─────┤                                                                 (4 pages)
NOAA active alerts  ─────┤
USGS earthquakes    ─────┼──► allianz_ext  ──────┘
GDACS catastrophes  ─────┤
ReliefWeb news      ─────┘
```

## Three Lines of Business

| LOB         | Products                                                                |
| ----------- | ----------------------------------------------------------------------- |
| Personal    | Auto · Home · Life · Renters · Umbrella                                 |
| Commercial  | Property · General Liability · Workers Comp · D&O · Professional Liability |
| Specialty   | Marine · Aviation · Cyber · Environmental Liability · Energy            |

## Gold Tables (Business Marts)

- `underwriting_kpis` — GWP/NWP/loss ratio/expense ratio/combined ratio by LOB & month
- `claims_summary` — claim frequency & severity by LOB/product/peril
- `exposure_accumulation` — TIV by state/CRESTA zone/LOB/product
- `solvency_capital` — Simplified Solvency II breakdown (SCR, MCR, own funds)
- `asset_portfolio_summary` — investments by asset class and rating
- `event_risk_correlation` — NOAA active alerts × per-state exposure
- `cat_event_pml` — catastrophe events with PML estimate

## Repository Layout

```
allianz-insurance-demo/
├── databricks.yml                  # DAB root
├── DEMO.md                         # Architecture & brand guidelines
├── TASKS.md                        # Build task tracker
├── README.md                       # This file
├── src/
│   ├── generate_data.py            # Synthetic data generator (Polars + Connect)
│   └── external_feeds.py           # Weather, NOAA, GDACS, USGS, ReliefWeb scraper
├── pipelines/
│   └── allianz_dlt.py              # Lakeflow Declarative Pipeline (bronze→silver→gold)
├── resources/
│   ├── jobs.yml                    # DAB job definitions
│   └── pipeline.yml                # DAB pipeline definition
├── dashboards/
│   └── allianz_dashboard.json      # Lakeview dashboard JSON
├── genie/                          # Genie config (populated via API)
└── scripts/
    ├── build_dashboard.py          # Creates the Lakeview dashboard
    └── build_genie.py              # Creates the Genie Space
```

## Running Locally

```bash
# 1. Authenticate
databricks auth login --host https://fevm-serverless-stable-xhky6g.cloud.databricks.com \
  --profile fe-vm-fevm-serverless-stable-xhky6g

# 2. Generate synthetic data
uv run --with polars --with numpy --with mimesis \
  --with "databricks-connect>=16.4,<17.0" \
  src/generate_data.py

# 3. Pull external feeds (weather, NOAA, GDACS, USGS, news)
uv run --with polars --with httpx \
  --with "databricks-connect>=16.4,<17.0" \
  src/external_feeds.py
```

## Deploying via DAB

```bash
databricks bundle validate --profile fe-vm-fevm-serverless-stable-xhky6g
databricks bundle deploy   --profile fe-vm-fevm-serverless-stable-xhky6g

# Trigger the full pipeline
databricks bundle run allianz_full_refresh --profile fe-vm-fevm-serverless-stable-xhky6g

# Build & deploy the dashboard
uv run --with httpx scripts/build_dashboard.py

# Build & deploy the Genie Space
uv run scripts/build_genie.py
```

## Brand Guidelines

| Element       | Value                                |
| ------------- | ------------------------------------ |
| Primary       | `#003781` (Allianz Blue)             |
| Secondary     | `#006192` (Allianz Light Blue)       |
| Accent        | `#96DCFA` Sky · `#FFAB00` Gold       |
| Font          | Allianz Neo (fallback: Inter)        |
| Voice         | Trustworthy, precise, regulator-aware |
