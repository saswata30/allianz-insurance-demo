# Allianz Demo — Build Tasks

## Status Legend
- [ ] pending
- [~] in-progress
- [x] complete

## 1. Workspace Setup
- [x] Authenticate to FEVM workspace `fevm-serverless-stable-xhky6g`
- [x] Confirm catalog `serverless_stable_xhky6g_catalog`
- [x] Start serverless warehouse `cf18de10632b58c8`
- [x] Create schemas: `allianz_bronze`, `allianz_silver`, `allianz_gold`, `allianz_ext`
- [x] Create UC volume `allianz_landing` for raw drops

## 2. Synthetic Data Generation
- [x] Customers (250K, 3 regions, demographics)
- [x] Policies (Personal/Commercial/Specialty, 600K total)
- [x] Premiums (12 months of monthly premium records)
- [x] Claims (PL/CL/SP with severity distributions)
- [x] Agents / Brokers
- [x] Reinsurance treaties
- [x] Asset portfolio (investments + ratings for solvency)
- [x] Geography reference (ZIP→CRESTA→country)

## 3. Bronze → Silver → Gold Pipeline (DLT)
- [x] Bronze ingest tables (raw landing)
- [x] Silver conformed dims & facts with DQ expectations
- [x] Gold business marts:
  - [x] `underwriting_kpis`
  - [x] `claims_summary`
  - [x] `exposure_accumulation`
  - [x] `solvency_capital`
  - [x] `asset_portfolio_summary`
  - [x] `event_risk_correlation`

## 4. External Real-Time Feeds
- [x] Open-Meteo weather observations job
- [x] NOAA storm events fetch
- [x] GDACS/USGS catastrophe events fetch
- [x] News/event scraping (ReliefWeb RSS)
- [x] Bronze → Silver geo-keyed transforms

## 5. Genie Space
- [x] Define Genie space pointing at gold schema
- [x] Curate ~12 sample questions
- [x] Add table-level instructions

## 6. AI/BI Dashboard
- [x] Executive KPI summary page
- [x] LOB drill-down (PL / CL / Specialty)
- [x] Exposure heatmap / accumulation page
- [x] Solvency II view
- [x] Real-time event correlation page

## 7. Databricks Asset Bundle
- [x] `databricks.yml` root config
- [x] Resources: jobs, pipeline, dashboard, schemas, volumes
- [x] Variables and target (dev → prod)
- [x] `databricks bundle validate`
- [x] `databricks bundle deploy`
- [x] `databricks bundle run` (data gen + DLT + external + dashboard)

## 8. GitHub
- [x] Initialize repo
- [x] `.gitignore` for python/databricks
- [x] README with run instructions
- [x] Create `saswata30/allianz-insurance-demo` via `gh`
- [x] Push to GitHub
