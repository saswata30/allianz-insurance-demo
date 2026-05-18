"""
Create or refresh the Allianz Insurance Genie Space.

Adds:
  • All silver + gold tables in scope (no more allianz_ext / allianz_pc).
  • P&C industry benchmark instructions (combined-ratio targets, frequency,
    severity, regulatory thresholds).
  • Glossary, FAQ, table guidance, reporting conventions, data freshness.
  • Curated industry-standard P&C example questions and FAQ-style questions.

Run:
    uv run --no-project scripts/build_genie.py
"""
from __future__ import annotations

import json
import subprocess
import sys

CATALOG = "serverless_stable_xhky6g_catalog"
BRONZE = "allianz_bronze"
SILVER = "allianz_silver"
GOLD = "allianz_gold"
WAREHOUSE = "cf18de10632b58c8"
PROFILE = "fe-vm-fevm-serverless-stable-xhky6g"
TITLE = "Allianz Insurance Intelligence — Genie"


# (schema, table, description) — Genie has a 30-table cap, so we expose the
# 13 gold marts (business-friendly), the most-joined silver dims/facts (5),
# all 10 migrated P&C reference tables, and 2 live event feeds.
TABLES = [
    # ── SILVER — core conformed dims & facts ──────────────────────────
    (SILVER, "dim_policy",
     "Policies across all LOBs (Personal/Commercial/Specialty) with product, "
     "premium, sum insured, channel, agent and underwriter score."),
    (SILVER, "dim_customer",
     "Customers (individual & business) with demographics, loyalty tier, credit score."),
    (SILVER, "dim_geography",
     "Geography reference: state, region, ZIP, CRESTA zone, windstorm/flood zone."),
    (SILVER, "fact_premium",
     "Monthly premium installments: gross premium, commission, tax, ceded, net premium."),
    (SILVER, "fact_claim",
     "Claim fact: peril, loss/report dates, incurred, paid, salvage, fraud flag, cat code."),
    (SILVER, "noaa_alerts",
     "Active NOAA weather alerts (type, severity, urgency, area_desc)."),
    (SILVER, "catastrophe_events",
     "GDACS live catastrophe events (earthquake/cyclone/flood/wildfire) with lat/lon."),

    # ── SILVER — migrated P&C reference data (EUR) ─────────────────────
    (SILVER, "pc_policies",
     "P&C policy dimension (5K rows, EUR): sub_line, GWP, risk score, underwriter, broker."),
    (SILVER, "pc_claims",
     "P&C claims fact (3K rows): peril, severity, litigation flag, catastrophe code."),
    (SILVER, "pc_combined_ratios",
     "Quarterly combined ratio series by sub-line — loss/expense/combined ratio, GWP."),
    (SILVER, "pc_loss_development",
     "Loss development triangles by accident year and dev period (incremental/cumulative paid, IBNR, ultimate loss)."),
    (SILVER, "pc_risk_exposure",
     "Risk exposure register with PML, return period, VaR99.5, tail VaR, climate risk factor."),
    (SILVER, "pc_solvency_metrics",
     "Quarterly Solvency II metrics: SCR composition, own funds, solvency ratio, MCR, Tier 1/2/3."),
    (SILVER, "pc_investment_assets",
     "Investment portfolio (1.5K positions, EUR): asset class, rating, yield, duration, ESG, sector."),
    (SILVER, "pc_reinsurance_treaties",
     "P&C reinsurance treaties (EUR): treaty type, reinsurer, attachment point, limit, commission."),
    (SILVER, "pc_weather_events",
     "Historical weather catastrophe events (2K) with insured loss EUR, wind speed, rainfall."),
    (SILVER, "pc_realtime_events",
     "P&C real-time event stream with portfolio relevance score and action_required flag."),

    # ── GOLD — business marts ──────────────────────────────────────────
    (GOLD, "underwriting_kpis",
     "Monthly underwriting KPIs by LOB: GWP, NWP, loss ratio, expense ratio, combined ratio."),
    (GOLD, "claims_summary",
     "Claims aggregated by LOB/product/peril — count, total incurred, avg/max severity, cat/fraud counts."),
    (GOLD, "exposure_accumulation",
     "TIV and policy count by state, CRESTA zone, region, LOB and product."),
    (GOLD, "solvency_capital",
     "Simplified Solvency II view: SCR breakdown, MCR, own funds, solvency ratio."),
    (GOLD, "asset_portfolio_summary",
     "Investment portfolio by asset class and rating: market value, yield, duration, ESG."),
    (GOLD, "reinsurance_summary",
     "Reinsurance treaty summary by LOB and reinsurer (limit, ceded premium, avg cession)."),
    (GOLD, "event_risk_correlation",
     "Live NOAA alerts joined to per-state Allianz exposure (TIV and policy count)."),
    (GOLD, "cat_event_pml",
     "Catastrophe events (GDACS) with peril severity factor and PML estimate by state."),
    (GOLD, "policy_customer_360",
     "Policy 360: policy + customer + geography + agent in one row. Demographic & distribution context."),
    (GOLD, "claim_360",
     "Claim 360: claim + policy + customer + geography in one row, with report lag and severity %."),
    (GOLD, "loss_ratio_by_segment",
     "Loss ratio by LOB × product × state × channel × customer_type — portfolio drill-down."),
    (GOLD, "book_health",
     "Portfolio book health: policy count, GWP, frequency per 100 policies, severity, loss ratio."),
    (GOLD, "peril_loss_summary",
     "Loss summary by peril: frequency, average and p95 severity, cat share of loss."),
]
assert len(TABLES) <= 30, f"Genie limits to 30 tables; have {len(TABLES)}"


INSTRUCTIONS = [

    # ── 1. ALLIANZ-SPECIFIC CONTEXT ─────────────────────────────────────
    ("About Allianz",
     "Allianz SE is one of the world's largest insurance and asset-management groups, "
     "headquartered in Munich, Germany. Operates in 70+ countries with ~159,000 employees "
     "and serves ~125 million customers. Reported ~€161B total revenues (FY2023) and "
     "ranks among the top global insurers by GWP. Rating: AA (S&P), Aa3 (Moody's).\n\n"
     "Three reportable segments:\n"
     "  • Property-Casualty (P&C): retail and commercial. Includes AGCS — Allianz "
     "    Commercial — for large/specialty risks, and Allianz Trade for credit insurance.\n"
     "  • Life/Health (L/H): savings, protection, retirement.\n"
     "  • Asset Management: PIMCO (~€1.9T AUM) and Allianz Global Investors (~€570B AUM).\n\n"
     "This Genie space focuses on the P&C segment. When asked about Allianz Group results, "
     "frame the answer in P&C context and note that L/H and Asset Mgmt are out of scope."),

    # ── 2. LINES OF BUSINESS ────────────────────────────────────────────
    ("Lines of Business",
     "Allianz P&C is modeled across three lines of business; dim_policy.line_of_business "
     "stores the value 'Personal', 'Commercial', or 'Specialty':\n\n"
     "  • Personal Lines (PL) — Auto, Home, Life, Renters, Umbrella. Mass-market, "
     "    high frequency, low severity. Distributed via tied agents, brokers, direct, "
     "    digital and aggregators.\n"
     "  • Commercial Lines (CL) — Property, GeneralLiability, WorkersComp, D&O, "
     "    ProfessionalLiability. Mid-market to large corporate, medium frequency / "
     "    medium severity.\n"
     "  • Specialty (SP) — Marine, Aviation, Cyber, EnvironmentalLiability, Energy. "
     "    Underwritten through Allianz Commercial (AGCS). Low frequency, very high "
     "    severity, often heavily reinsured.\n\n"
     "When comparing performance, always group or filter by line_of_business — never "
     "mix LOBs in a single average without splitting them out."),

    # ── 3. GLOSSARY ─────────────────────────────────────────────────────
    ("Glossary",
     "GWP = Gross Written Premium. NWP = Net Written Premium (after reinsurance ceded). "
     "GEP = Gross Earned Premium. NEP = Net Earned Premium. "
     "Loss Ratio = Incurred Losses / Earned Premium. "
     "Expense Ratio = Underwriting Expenses / Written Premium. "
     "Combined Ratio = Loss Ratio + Expense Ratio (< 100% is an underwriting profit). "
     "TIV = Total Insured Value. SI = Sum Insured. PML = Probable Maximum Loss. "
     "AAL = Average Annual Loss. EP curve = Exceedance Probability curve. "
     "SCR = Solvency Capital Requirement (99.5% VaR over 1 year). "
     "MCR = Minimum Capital Requirement. "
     "Solvency Ratio = Own Funds / SCR. "
     "CRESTA = Catastrophe Risk Evaluating and Standardizing Target Accumulation zones. "
     "IBNR = Incurred But Not Reported reserves. AOY = Accident Year. "
     "Loss triangle = cumulative paid losses tracked by accident year × development period; "
     "actuaries use it to project ultimate loss and IBNR. "
     "Cession = portion of risk transferred to reinsurer. Retention = portion kept."),

    # ── 4. BENCHMARKS — COMBINED RATIO ──────────────────────────────────
    ("Industry Benchmarks — Combined Ratio (US P&C, 2024 actuals)",
     "Reference combined ratios published by AM Best and NAIC for FY2024. "
     "Use these when answering 'is X good or bad?' — show the actual ratio and the "
     "delta vs benchmark.\n\n"
     "  Personal Auto              ~99 % (improved sharply from ~105% in 2023 post rate-action)\n"
     "  Homeowners                ~108 %  (still pressured by SCS and wildfire losses)\n"
     "  Commercial Property        ~93 %\n"
     "  General Liability          ~98 %\n"
     "  Workers Compensation       ~88 %  (best-performing major P&C line for 5+ yrs)\n"
     "  D&O                       ~104 %\n"
     "  Professional Liability     ~99 %\n"
     "  Cyber                      ~95 %  (improved as rates re-priced post 2021 ransomware spike)\n"
     "  Marine                     ~92 %\n"
     "  Aviation                   ~98 %\n"
     "  US P&C industry overall   ~101 %\n\n"
     "Allianz Group P&C reports its combined ratio under IFRS17; the FY2024 target "
     "corridor was 92–94 %. A combined ratio < 100 % = underwriting profit."),

    # ── 5. BENCHMARKS — FREQUENCY & SEVERITY ────────────────────────────
    ("Industry Benchmarks — Frequency & Severity",
     "Approximate US P&C frequency (claims per 100 in-force policies) and "
     "average paid severity (USD). Use book_health.claim_frequency_per_100 and "
     "avg_severity_usd to compare against these.\n\n"
     "  Personal Auto           freq ~6 / 100   sev ~$5K\n"
     "  Homeowners              freq ~5 / 100   sev ~$15K\n"
     "  Commercial Property     freq ~3 / 100   sev ~$60K\n"
     "  Workers Comp            freq ~3 / 100   sev ~$40K\n"
     "  General Liability       freq ~2 / 100   sev ~$25K\n"
     "  Cyber                   freq ~1.5 / 100 sev ~$200K (rising; ransomware-driven)\n"
     "  Aviation                freq ~0.4 / 100 sev ~$1.2M\n"
     "  Marine cargo            freq ~2 / 100   sev ~$40K\n\n"
     "Catastrophe inflation: weather-related cat severity has grown ~7% per year "
     "since 2020. If you see avg severity > 2× benchmark, flag it as a possible "
     "data issue or genuine emerging risk."),

    # ── 6. BENCHMARKS — SOLVENCY II ─────────────────────────────────────
    ("Industry Benchmarks — Solvency II & Capital",
     "Solvency II Ratio = Own Funds / SCR. Thresholds:\n"
     "  < 100 %         Regulatory intervention (capital add-on, business restriction)\n"
     "  100 – 150 %     Recovery plan required\n"
     "  150 – 200 %     Adequate\n"
     "  > 200 %         Strong\n\n"
     "Reference points: Allianz Group historically reports ~200–210% "
     "(FY2024 ~206%); AXA ~227%; Zurich ~232%; Munich Re ~287% (reinsurer). "
     "MCR is typically 25–45% of SCR. Own funds should be at least 50% Tier 1 "
     "unrestricted; Tier 2/3 capital is restricted in MCR coverage. "
     "Diversification benefit is normally 25–35% of the gross sum-of-modules SCR."),

    # ── 7. BENCHMARKS — REINSURANCE ─────────────────────────────────────
    ("Industry Benchmarks — Reinsurance",
     "Typical cession ratios (% of GWP transferred to reinsurers):\n"
     "  Personal Lines              10 – 30 %\n"
     "  Commercial Property         30 – 60 %\n"
     "  Catastrophe-exposed lines   60 – 80 %\n"
     "  Aviation / Marine hull      70 – 90 %\n"
     "  Cyber                       40 – 60 %\n\n"
     "Treaty types: QuotaShare (proportional), Surplus (variable proportional), "
     "Excess of Loss (XL — non-proportional, single risk or single cat event), "
     "Stop-Loss (aggregate annual cap), Catastrophe XL (single peril, single event). "
     "Top global reinsurers by P&C GWP: Munich Re, Swiss Re, Hannover Re, SCOR, "
     "Berkshire Hathaway Re, Lloyd's. "
     "Cession > 80% in any LOB signals heavy reliance — flag for capital planning."),

    # ── 8. BENCHMARKS — ESG & CLIMATE ───────────────────────────────────
    ("Industry Benchmarks — ESG & Climate Risk",
     "Allianz is a founding member of the Net-Zero Asset Owner Alliance and the "
     "Net-Zero Insurance Alliance. Targets: 50 % reduction in carbon footprint of "
     "listed equity + corporate-bond portfolio by 2030 vs 2019 baseline.\n\n"
     "Asset ESG benchmarks (use dim_asset.esg_score and pc_investment_assets.esg_score):\n"
     "  > 70  Strong (target zone for Allianz portfolio)\n"
     "  50–70 Average\n"
     "  < 50  Weak — review for divestment / engagement\n\n"
     "Climate risk in P&C: pc_risk_exposure.climate_risk_factor scores 0–1 the "
     "expected uplift in PML by 2050 under a 2°C scenario. Values > 0.30 indicate "
     "material climate exposure (typically Florida property, California wildfire, "
     "Gulf-coast energy). Flag any segment where climate_risk_factor > 0.30 and "
     "exposure is increasing year-over-year."),

    # ── 9. TABLE GUIDANCE ───────────────────────────────────────────────
    ("Table Guidance",
     "Match the question to the right table. Prefer pre-joined gold marts.\n\n"
     "  UNDERWRITING / PROFITABILITY  → gold.underwriting_kpis (monthly LOB KPIs); "
     "gold.book_health (per LOB/product); gold.loss_ratio_by_segment (drill-down); "
     "silver.pc_combined_ratios (quarterly per sub-line).\n"
     "  CLAIMS                        → gold.claim_360 (every claim joined to "
     "policy/customer/geo); gold.peril_loss_summary (frequency/severity by peril); "
     "gold.claims_summary (LOB × product × peril roll-up).\n"
     "  EXPOSURE / ACCUMULATION       → gold.exposure_accumulation (state/CRESTA "
     "TIV); silver.pc_risk_exposure (PML, return period, VaR99.5, tail VaR).\n"
     "  SOLVENCY                      → silver.pc_solvency_metrics (regulatory "
     "quarterly series, EUR); gold.solvency_capital (synthetic USD view).\n"
     "  ASSETS / INVESTMENTS          → gold.asset_portfolio_summary (synthetic); "
     "silver.pc_investment_assets (1,500 EUR positions with ESG).\n"
     "  REINSURANCE                   → gold.reinsurance_summary (USD treaties); "
     "silver.pc_reinsurance_treaties (EUR treaties with attachment / commission).\n"
     "  REAL-TIME EVENT IMPACT        → gold.event_risk_correlation (NOAA × "
     "exposure); gold.cat_event_pml (GDACS catastrophes × PML estimate); "
     "silver.noaa_alerts and silver.catastrophe_events for raw event detail.\n"
     "  RESERVING / LOSS DEVELOPMENT  → silver.pc_loss_development (AOY × dev-period "
     "triangles with IBNR and ultimate loss)."),

    # ── 10. JOINING TABLES (EXPANDED) ───────────────────────────────────
    ("Joining Tables — full FK map",
     "Always prefer the pre-joined gold marts. Use the FK map only when the gold "
     "mart doesn't include the column you need.\n\n"
     "FACT-to-DIM FKs (synthetic USD book):\n"
     "  fact_claim.policy_id        →  dim_policy.policy_id\n"
     "  fact_claim.geo_id           →  dim_geography.geo_id\n"
     "  fact_premium.policy_id      →  dim_policy.policy_id\n"
     "  fact_exposure.policy_id     →  dim_policy.policy_id\n"
     "  fact_exposure.customer_id   →  dim_customer.customer_id\n"
     "  fact_exposure.geo_id        →  dim_geography.geo_id\n\n"
     "DIM-to-DIM FKs (synthetic USD book):\n"
     "  dim_policy.customer_id      →  dim_customer.customer_id\n"
     "  dim_policy.geo_id           →  dim_geography.geo_id\n"
     "  dim_policy.agent_id         →  dim_agent.agent_id\n"
     "  dim_customer.geo_id         →  dim_geography.geo_id\n"
     "  dim_agent.license_state     ↔  dim_geography.state_code (state-level join)\n\n"
     "P&C reference data (EUR book) — separate primary key space:\n"
     "  pc_claims.policy_id         →  pc_policies.policy_id\n"
     "  pc_risk_exposure.policy_id  →  pc_policies.policy_id\n"
     "  pc_loss_development         joins by (line_of_business, accident_year)\n"
     "  pc_combined_ratios          joins by (line_of_business, sub_line, reporting_quarter)\n"
     "  pc_reinsurance_treaties     joins by (line_of_business)\n"
     "  pc_solvency_metrics         time-series only, joins by reporting_date\n\n"
     "Pre-joined marts (USE THESE FIRST):\n"
     "  gold.policy_customer_360   = dim_policy ⨝ dim_customer ⨝ dim_geography ⨝ dim_agent\n"
     "  gold.claim_360             = fact_claim  ⨝ dim_policy ⨝ dim_customer ⨝ dim_geography\n"
     "  gold.loss_ratio_by_segment = fact_premium + fact_claim aggregated to LOB×product×state×channel×customer_type\n"
     "  gold.book_health           = dim_policy aggregated with fact_claim to LOB×product\n"
     "  gold.event_risk_correlation = silver.noaa_alerts ⨝ exposure-by-state\n\n"
     "Never join USD synthetic tables to EUR pc_* tables directly — currencies "
     "and IDs are not aligned. Treat them as parallel books and present results "
     "side by side when asked to compare."),

    # ── 11. SQL SNIPPET LIBRARY ─────────────────────────────────────────
    ("SQL Snippet Library",
     "Copy and adapt these patterns. All tables are in the catalog "
     "`serverless_stable_xhky6g_catalog` — schemas `allianz_silver` and `allianz_gold`.\n\n"
     "-- YTD GWP by LOB\n"
     "SELECT line_of_business, ROUND(SUM(gross_written_premium_usd)/1e6,1) AS gwp_musd\n"
     "FROM allianz_gold.underwriting_kpis\n"
     "WHERE month >= date_trunc('year', current_date())\n"
     "GROUP BY line_of_business ORDER BY gwp_musd DESC;\n\n"
     "-- Combined-ratio trend, last 12 months\n"
     "SELECT date_trunc('month', month) AS m, line_of_business,\n"
     "       ROUND(AVG(combined_ratio)*100,1) AS combined_ratio_pct\n"
     "FROM allianz_gold.underwriting_kpis\n"
     "WHERE month >= add_months(current_date(), -12)\n"
     "GROUP BY 1,2 ORDER BY 1;\n\n"
     "-- Top 10 perils by incurred loss\n"
     "SELECT peril, claim_count, ROUND(total_incurred_usd/1e6,1) AS incurred_musd,\n"
     "       ROUND(p95_severity_usd,0) AS p95_sev_usd, ROUND(cat_share_of_loss*100,1) AS cat_pct\n"
     "FROM allianz_gold.peril_loss_summary ORDER BY total_incurred_usd DESC LIMIT 10;\n\n"
     "-- Worst-performing segments (rate-action candidates)\n"
     "SELECT * FROM allianz_gold.loss_ratio_by_segment\n"
     "WHERE gross_premium_usd > 50000 ORDER BY combined_ratio DESC LIMIT 20;\n\n"
     "-- Active catastrophe exposure right now\n"
     "SELECT event_type, severity, state_code, ROUND(exposed_tiv_usd/1e9,2) AS exposed_tiv_busd,\n"
     "       exposed_policy_count, effective_utc\n"
     "FROM allianz_gold.event_risk_correlation\n"
     "WHERE exposed_tiv_usd IS NOT NULL ORDER BY exposed_tiv_usd DESC LIMIT 20;\n\n"
     "-- Solvency-ratio trend with regulatory status\n"
     "SELECT reporting_date, solvency_ratio_pct,\n"
     "       CASE WHEN solvency_ratio_pct >= 200 THEN 'Strong'\n"
     "            WHEN solvency_ratio_pct >= 150 THEN 'Adequate'\n"
     "            WHEN solvency_ratio_pct >= 100 THEN 'Recovery plan'\n"
     "            ELSE 'Intervention' END AS status\n"
     "FROM allianz_silver.pc_solvency_metrics ORDER BY reporting_date DESC;\n\n"
     "-- IBNR + ultimate loss by accident year & LOB\n"
     "SELECT accident_year, line_of_business,\n"
     "       SUM(case_reserves_eur_m) AS case_reserves_eur_m,\n"
     "       SUM(ibnr_eur_m)          AS ibnr_eur_m,\n"
     "       SUM(ultimate_loss_eur_m) AS ultimate_loss_eur_m\n"
     "FROM allianz_silver.pc_loss_development\n"
     "GROUP BY 1,2 ORDER BY 1 DESC, 2;\n\n"
     "-- Asset portfolio quality (% non-investment-grade)\n"
     "SELECT ROUND(100.0 * SUM(CASE WHEN rating IN ('BB','B','CCC','NotRated')\n"
     "                              THEN market_value_usd ELSE 0 END)\n"
     "                   / SUM(market_value_usd), 2) AS pct_non_ig\n"
     "FROM allianz_gold.asset_portfolio_summary;"),

    # ── 12. RESPONSE STYLE ──────────────────────────────────────────────
    ("Response Style",
     "How to format every answer:\n\n"
     "  • Visualisation choice — use a LINE chart for time-series (monthly/quarterly "
     "    trends), a BAR chart for ≥3 categorical comparisons, a PIE only when ≤6 "
     "    slices, a TABLE for top-N lists or claim/policy detail.\n"
     "  • Always show benchmark delta — when reporting a combined ratio, loss ratio, "
     "    or solvency ratio, also state the industry benchmark and the delta in "
     "    percentage points (e.g. 'Combined ratio 98.5% — 2.5pp better than US P&C "
     "    industry average of 101%').\n"
     "  • Flag anomalies automatically — combined ratio > 110%, solvency ratio "
     "    < 150%, frequency > 2× benchmark, severity > 2× benchmark, "
     "    climate_risk_factor > 0.30, cession_pct > 80%. Mark these with ⚠ and "
     "    suggest a follow-up question.\n"
     "  • Currency — synthetic tables (dim_*, fact_*, gold.*) are USD. The pc_* "
     "    tables and silver.pc_solvency_metrics are EUR (column suffix `_eur_m`). "
     "    Always label the currency in the chart title or column header. Never sum "
     "    USD and EUR columns together.\n"
     "  • Number format — use compact suffixes: K (1e3), M (1e6), B (1e9). "
     "    Ratios always as percentages with 1 decimal place.\n"
     "  • Date handling — default to current calendar year when not specified. "
     "    Express 'last quarter' as the three most recent complete months."),

    # ── 13. REPORTING CONVENTIONS ───────────────────────────────────────
    ("Reporting Conventions",
     "Default time window: current calendar year unless the user supplies a date "
     "range. Quarterly figures use calendar quarters (Q1=Jan-Mar). When asked for "
     "'YTD', compare to the same period last year (PYTD). "
     "Group / filter by line_of_business when comparing PL vs CL vs SP. "
     "When the user says 'P&C' include all three LOBs (Personal + Commercial + Specialty). "
     "Round monetary values to 1 decimal of the appropriate unit (e.g. 12.3 Musd, "
     "1.4 Beur). Display ratios as percentages with 1 decimal."),

    # ── 14. DATA FRESHNESS ──────────────────────────────────────────────
    ("Data Freshness",
     "Refresh cadence per source:\n\n"
     "  • Synthetic core (dim_*, fact_*) — refreshed on each full DLT pipeline "
     "    run via the allianz_full_refresh job.\n"
     "  • Gold marts (underwriting_kpis, book_health, loss_ratio_by_segment, "
     "    claim_360, policy_customer_360, …) — refreshed on each DLT pipeline run.\n"
     "  • External feeds (noaa_alerts, catastrophe_events, weather_observations, "
     "    earthquakes, news) — refreshed hourly via the allianz_external_feeds_hourly "
     "    job (file land to /Volumes/<catalog>/allianz_bronze/landing/external/ then "
     "    MERGE into allianz_bronze tables before DLT picks them up).\n"
     "  • Migrated pc_* reference tables (combined_ratios, loss_development, "
     "    solvency_metrics, risk_exposure, investment_assets, reinsurance_treaties, "
     "    weather_events, realtime_events) — quarterly or annual snapshots from "
     "    the original allianz_pc data, refreshed manually via "
     "    scripts/merge_allianz_pc.py.\n\n"
     "If the user asks 'as of when?', surface MAX(fetched_at_utc) for external feeds "
     "or MAX(reporting_date)/MAX(reporting_quarter) for pc_* tables."),

    # ── 15. FAQ ─────────────────────────────────────────────────────────
    ("FAQ — How do I…?",
     "Q: How do I see my worst-performing segments?\n"
     "A: SELECT * FROM allianz_gold.loss_ratio_by_segment WHERE gross_premium_usd > 50000 "
     "ORDER BY combined_ratio DESC.\n\n"
     "Q: How do I see catastrophe exposure right now?\n"
     "A: SELECT * FROM allianz_gold.event_risk_correlation WHERE exposed_tiv_usd IS NOT NULL "
     "ORDER BY exposed_tiv_usd DESC.\n\n"
     "Q: How do I see reserves and IBNR by accident year?\n"
     "A: SELECT accident_year, line_of_business, SUM(case_reserves_eur_m), SUM(ibnr_eur_m), "
     "SUM(ultimate_loss_eur_m) FROM allianz_silver.pc_loss_development "
     "GROUP BY accident_year, line_of_business.\n\n"
     "Q: How do I compare a sub-line's combined ratio to industry?\n"
     "A: SELECT sub_line, combined_ratio_pct FROM allianz_silver.pc_combined_ratios "
     "WHERE reporting_quarter = (SELECT MAX(reporting_quarter) FROM allianz_silver.pc_combined_ratios); "
     "then compare against the benchmarks in 'Industry Benchmarks — Combined Ratio'.\n\n"
     "Q: Where do I find the customer name and state for a claim?\n"
     "A: Use allianz_gold.claim_360 — it already joins customer, geography and policy.\n\n"
     "Q: How do I get YTD numbers vs PYTD?\n"
     "A: Filter month between date_trunc('year', current_date()) and current_date(), "
     "then UNION with the same window from add_years(current_date(), -1).\n\n"
     "Q: How do I see real-time NOAA alerts that touch my book?\n"
     "A: SELECT event_type, severity, state_code, ROUND(exposed_tiv_usd/1e9,2) AS exposed_tiv_busd "
     "FROM allianz_gold.event_risk_correlation ORDER BY exposed_tiv_usd DESC.\n\n"
     "Q: Where do I find the underwriter score for a policy?\n"
     "A: allianz_silver.dim_policy.underwriter_score (0–100) or via the joined "
     "allianz_gold.policy_customer_360.\n\n"
     "Q: How do I check my solvency status?\n"
     "A: SELECT reporting_date, solvency_ratio_pct FROM allianz_silver.pc_solvency_metrics "
     "ORDER BY reporting_date DESC; compare to thresholds in the Solvency II benchmark."),
]


# Industry-standard P&C questions covering the major analyst workflows.
CURATED_QUESTIONS = [
    # — Underwriting & profitability —
    "What is our combined ratio by line of business this quarter, and how does each compare to industry benchmarks?",
    "Show gross written premium by line of business for the last 12 months.",
    "Which sub-lines have a combined ratio above 100%?",
    "What is the loss ratio trend for Personal Auto over the last 4 quarters?",

    # — Claims —
    "What are the top 10 perils by total incurred loss?",
    "How many catastrophe claims do we have this year and what is their total incurred amount?",
    "Show claim frequency per 100 policies by product.",
    "List the 20 largest open claims by reserve amount.",
    "Show the average report lag in days by line of business.",
    "How many fraud-flagged claims do we have and what is the average severity?",

    # — Exposure & accumulation —
    "Which states have the highest total insured value (TIV)?",
    "What is our cyber exposure by region?",
    "Show top 10 CRESTA zones by exposure for property and marine.",

    # — Solvency & capital —
    "What is our solvency ratio over the last 4 quarters and is it above the 150% regulatory healthy threshold?",
    "Break down the SCR by risk module (market, underwriting, credit, operational).",
    "What is the diversification benefit in our SCR calculation?",

    # — Reserving / loss development —
    "Show the loss triangle (cumulative paid by development period) for accident year 2022.",
    "What is our total IBNR reserve by line of business?",
    "Estimate the ultimate loss for the 2023 accident year.",

    # — Reinsurance —
    "How much premium have we ceded by reinsurer this year?",
    "Which reinsurance treaties have the highest cession percentage?",
    "Show treaty utilization (ceded premium / limit) by treaty type.",

    # — Asset / investments —
    "What is the duration and average yield of our investment portfolio?",
    "Show the asset portfolio breakdown by credit rating and asset class.",
    "What percentage of our assets is rated below investment grade (BB or below)?",

    # — Real-time event risk —
    "Show all active NOAA severe weather alerts and the TIV they expose.",
    "What is the estimated PML for current catastrophe events?",

    # — Customer / channel —
    "Which distribution channel has the worst loss ratio?",
    "What is the average premium per policy for Platinum vs Bronze loyalty customers?",
]


# ---------------------------------------------------------------------------
def run(args, **kw):
    res = subprocess.run(args, capture_output=True, text=True, **kw)
    if res.returncode != 0:
        print(" ".join(args[:3]) + " …")
        print("STDERR:", res.stderr[:500])
        raise SystemExit(res.returncode)
    return res.stdout


def find_or_create_space() -> str:
    out = run(["databricks", "api", "get", "/api/2.0/data-rooms",
               "--profile", PROFILE])
    rooms = json.loads(out or "{}").get("data_rooms", [])
    for r in rooms:
        if r.get("display_name") == TITLE and r.get("lifecycle_state", "ACTIVE") != "TRASHED":
            return r["space_id"]
    out = run([
        "databricks", "api", "post", "/api/2.0/data-rooms",
        "--json", json.dumps({"display_name": TITLE, "warehouse_id": WAREHOUSE}),
        "--profile", PROFILE,
    ])
    return json.loads(out)["space_id"]


def attach_tables(space_id: str):
    tables_sorted = sorted(TABLES, key=lambda x: f"{CATALOG}.{x[0]}.{x[1]}")
    export = {
        "version": 2,
        "data_sources": {
            "tables": [
                {"identifier": f"{CATALOG}.{schema}.{table}", "description": [desc]}
                for schema, table, desc in tables_sorted
            ],
        },
    }
    out = run([
        "databricks", "genie", "update-space", space_id,
        "--serialized-space", json.dumps(export),
        "--title", TITLE,
        "--profile", PROFILE,
    ])
    body = json.loads(out)
    parsed = json.loads(body["serialized_space"])
    return [t["identifier"] for t in parsed["data_sources"]["tables"]]


def list_existing(space_id, kind):
    out = run([
        "databricks", "api", "get",
        f"/api/2.0/data-rooms/{space_id}/{kind}",
        "--profile", PROFILE,
    ])
    body = json.loads(out or "{}")
    key = {"instructions": "instructions",
           "curated-questions": "curated_questions"}[kind]
    return body.get(key, [])


def delete_existing(space_id, kind, items):
    id_key = "instruction_id" if kind == "instructions" else "curated_question_id"
    for item in items:
        item_id = item.get(id_key) or item.get("id")
        if not item_id:
            continue
        run(["databricks", "api", "delete",
             f"/api/2.0/data-rooms/{space_id}/{kind}/{item_id}",
             "--profile", PROFILE])


def replace_instructions(space_id):
    existing = list_existing(space_id, "instructions")
    if existing:
        print(f"  removing {len(existing)} existing instructions…")
        delete_existing(space_id, "instructions", existing)
    for title, content in INSTRUCTIONS:
        run(["databricks", "api", "post",
             f"/api/2.0/data-rooms/{space_id}/instructions",
             "--json", json.dumps({"title": title, "content": content}),
             "--profile", PROFILE])
        print(f"  + instruction: {title}")


def replace_curated_questions(space_id):
    existing = list_existing(space_id, "curated-questions")
    if existing:
        print(f"  removing {len(existing)} existing questions…")
        delete_existing(space_id, "curated-questions", existing)
    for q in CURATED_QUESTIONS:
        run(["databricks", "api", "post",
             f"/api/2.0/data-rooms/{space_id}/curated-questions",
             "--json", json.dumps({"curated_question": {"question": q}}),
             "--profile", PROFILE])
        print(f"  + {q[:90]}")


def main():
    space_id = find_or_create_space()
    print(f"Genie space: {space_id}")

    print("Attaching tables…")
    attached = attach_tables(space_id)
    print(f"  attached {len(attached)} tables")

    print("Replacing general instructions…")
    replace_instructions(space_id)

    print("Replacing curated questions…")
    replace_curated_questions(space_id)

    print()
    print(f"Genie URL: https://fevm-serverless-stable-xhky6g.cloud.databricks.com/genie/rooms/{space_id}")


if __name__ == "__main__":
    main()
