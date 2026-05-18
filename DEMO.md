# Allianz Insurance Intelligence Platform — Databricks Demo

## Customer
**Allianz SE** — Global insurance & asset management leader (Munich, Germany). One of the world's largest insurers by revenue, operating across 70+ countries with three core lines of business and an asset-management arm (Allianz Global Investors, PIMCO).

## Brand Guidelines

| Element       | Value                                                                |
| ------------- | -------------------------------------------------------------------- |
| Primary       | `#003781` (Allianz Blue)                                             |
| Secondary     | `#006192` (Allianz Light Blue)                                       |
| Accent        | `#96DCFA` (Sky Blue), `#FFFFFF` (white background)                   |
| Text          | `#1A1A1A` on white, `#FFFFFF` on blue                                |
| Font          | Allianz Neo (fallback: Inter / sans-serif)                           |
| Logo motif    | Blue eagle, single-color treatment in dashboards                     |
| Voice         | Trustworthy, precise, globally consistent, regulator-aware (Solvency II) |

## Business Lines Modeled

### 1. Personal Lines (PL)
- **Auto** — private passenger, telematics, comprehensive/collision
- **Home** — homeowners, renters, condo
- **Life** — term, whole, variable, annuities

### 2. Commercial Lines (CL)
- **Property** — commercial real estate, business interruption
- **General Liability** — CGL, D&O, E&O, professional liability
- **Workers' Compensation** — by industry classification

### 3. Specialty Insurance (SP)
- **Marine** — hull, cargo, P&I
- **Aviation** — hull, liability, war risk
- **Cyber** — first-party, third-party, ransomware, BI

## Demo Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         SOURCES                                              │
│  ┌────────────────────┐  ┌──────────────────┐  ┌────────────────────────┐    │
│  │ Synthetic Allianz  │  │ NOAA / Open-     │  │ GDACS / ReliefWeb /    │    │
│  │ Core Systems       │  │ Meteo Weather    │  │ USGS catastrophe feeds │    │
│  │ (policies, claims) │  │ APIs             │  │ (web event scraper)    │    │
│  └─────────┬──────────┘  └────────┬─────────┘  └───────────┬────────────┘    │
└────────────┼─────────────────────┼──────────────────────────┼────────────────┘
             ▼                     ▼                          ▼
        ╔══════════════════════════════════════════════════════════════╗
        ║                    UNITY CATALOG                             ║
        ║  catalog: serverless_stable_xhky6g_catalog                   ║
        ║  schemas: allianz_bronze / _silver / _gold / _ext            ║
        ╚══════════════════════════════════════════════════════════════╝
             ▼                     ▼                          ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  BRONZE (raw landing)                                       │
        │  - policies_raw, claims_raw, customers_raw, premiums_raw    │
        │  - weather_obs_raw, catastrophe_events_raw, news_raw        │
        └────────────────────────┬────────────────────────────────────┘
                                 ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  SILVER (conformed, deduped, typed) — Lakeflow DLT          │
        │  - dim_customer, dim_policy, dim_agent, dim_geography       │
        │  - fact_premium, fact_claim, fact_exposure                  │
        │  - weather_obs, catastrophe_events (geo-keyed)              │
        └────────────────────────┬────────────────────────────────────┘
                                 ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  GOLD (business marts)                                      │
        │  - underwriting_kpis  (GWP, NWP, loss ratio, combined ratio)│
        │  - claims_summary     (frequency, severity by LOB)          │
        │  - exposure_accumulation (TIV by ZIP/CRESTA)                │
        │  - solvency_capital   (SCR, MCR, own funds)                 │
        │  - asset_portfolio    (investments, AAA/BBB/etc.)           │
        │  - event_risk_correlation  ◀── joins weather+catastrophe    │
        │                              with exposure for PML/scenario │
        └────────────────────────┬────────────────────────────────────┘
                                 ▼
                ┌──────────────────────────────────┐
                │  CONSUMPTION LAYER               │
                │  • Genie Space (NL Q&A)          │
                │  • AI/BI Dashboard               │
                └──────────────────────────────────┘
```

## Components

| Component                         | Tech                                          |
| --------------------------------- | --------------------------------------------- |
| Synthetic data generator          | Polars + Mimesis on Serverless                |
| Bronze landing                    | UC Volume (parquet) → managed Delta           |
| Bronze→Silver→Gold transformation | Lakeflow Declarative Pipeline (DLT, serverless) |
| Real-time weather feed            | Job task hitting Open-Meteo + NOAA APIs       |
| Catastrophe event feed            | Job task scraping GDACS RSS + USGS earthquakes |
| Correlation engine                | SQL gold view joining geography + perils      |
| NL Q&A                            | Genie Space on gold schema                    |
| Dashboard                         | Lakeview (AI/BI) dashboard                    |
| Orchestration                     | Databricks Workflow / Lakeflow Job            |
| Packaging                         | Databricks Asset Bundle (databricks.yml)      |
| Version control                   | GitHub — saswata30/allianz-insurance-demo     |

## Genie Space — Curated Questions

- "What is the gross written premium by line of business this quarter?"
- "Show combined ratio trend across Personal, Commercial, Specialty lines."
- "Which states have the highest property exposure accumulation?"
- "What's our PML for a Cat-3 hurricane in Florida?"
- "How many open cyber claims do we have, and what's the average severity?"
- "What is the solvency capital ratio?"
- "How is the asset portfolio diversified by rating?"
- "Did any active catastrophe events overlap our policy footprint last week?"
- "Show claims frequency by LOB before and after recent weather events."

## Workspace & Resources

- **Workspace**: `https://fevm-serverless-stable-xhky6g.cloud.databricks.com`
- **Catalog**: `serverless_stable_xhky6g_catalog` (FEVM-managed)
- **Schemas**: `allianz_bronze`, `allianz_silver`, `allianz_gold`, `allianz_ext`
- **Warehouse**: `Serverless Starter Warehouse` (`cf18de10632b58c8`)
- **Compute**: Serverless throughout (no classic clusters)
- **GitHub**: `https://github.com/saswata30/allianz-insurance-demo`
