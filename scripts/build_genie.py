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
    ("Lines of Business",
     "Allianz operates three lines of business: "
     "Personal Lines (Auto, Home, Life, Renters, Umbrella); "
     "Commercial Lines (Property, GeneralLiability, WorkersComp, D&O, ProfessionalLiability); "
     "Specialty (Marine, Aviation, Cyber, EnvironmentalLiability, Energy). "
     "dim_policy.line_of_business holds 'Personal', 'Commercial', 'Specialty'."),

    ("Glossary",
     "GWP = Gross Written Premium. NWP = Net Written Premium (after reinsurance ceded). "
     "TIV = Total Insured Value. SCR = Solvency Capital Requirement. MCR = Minimum Capital Requirement. "
     "Combined Ratio = Loss Ratio + Expense Ratio (<100% is profitable). "
     "PML = Probable Maximum Loss. CRESTA = Catastrophe Risk Evaluating and Standardizing Target Accumulation. "
     "IBNR = Incurred But Not Reported. AOY = Accident Year. "
     "Loss triangle = paid losses tracked across development periods to estimate ultimate loss."),

    ("Industry Benchmarks — P&C Combined Ratio (US, AM Best 2024)",
     "Use these reference numbers when answering 'is this good / bad': "
     "Personal Auto ~104%; Homeowners ~110%; Commercial Property ~95%; "
     "Workers Comp ~85%; General Liability ~98%; D&O ~104%; "
     "Cyber ~105%; Marine ~92%; Aviation ~98%; Industry overall ~101%. "
     "A combined ratio below 100% indicates an underwriting profit."),

    ("Industry Benchmarks — Claim Frequency & Severity",
     "Personal Auto: ~6 claims per 100 policies, avg severity ~$5K. "
     "Homeowners: ~5 per 100, severity ~$15K. "
     "Commercial Property: ~3 per 100, severity ~$60K. "
     "Workers Comp: ~3 per 100, severity ~$40K. "
     "Cyber: ~1.5 per 100, severity ~$200K (and rising). "
     "Use book_health.claim_frequency_per_100 and avg_severity_usd to compare."),

    ("Industry Benchmarks — Solvency II",
     "Solvency Ratio thresholds: <100% triggers regulatory intervention; "
     "100–150% requires a recovery plan; >150% is healthy. "
     "Allianz Group historically targets ~200%. "
     "MCR (Minimum Capital Requirement) is typically 25–45% of SCR. "
     "Own funds must cover SCR with diversified Tier 1/2/3 capital."),

    ("Industry Benchmarks — Reinsurance",
     "Typical ceded ratio: 10–30% of GWP for Personal Lines, 30–60% for "
     "Property/Specialty, 70%+ for Aviation and Catastrophe risks. "
     "Treaty types: QuotaShare (proportional), Excess of Loss (XL, non-proportional), "
     "Stop-Loss (annual aggregate cap), Catastrophe XL (single peril). "
     "Cession % > 80% in any LOB suggests heavy reliance on reinsurance capacity."),

    ("Table Guidance",
     "For UNDERWRITING / PROFITABILITY: use gold.underwriting_kpis (monthly KPIs), "
     "gold.book_health (per LOB/product), gold.loss_ratio_by_segment (drill-down), "
     "and silver.pc_combined_ratios (quarterly per sub-line). "
     "For CLAIMS: use gold.claim_360 (every claim, joined), gold.peril_loss_summary "
     "(frequency/severity by peril), gold.claims_summary (LOB/product/peril rollup). "
     "For EXPOSURE / ACCUMULATION: use gold.exposure_accumulation (state/CRESTA), "
     "silver.pc_risk_exposure (PML, return period, tail VaR). "
     "For SOLVENCY: use silver.pc_solvency_metrics (regulatory series, EUR) and "
     "gold.solvency_capital (synthetic toy model, USD). "
     "For ASSETS: use gold.asset_portfolio_summary and silver.pc_investment_assets. "
     "For REINSURANCE: use gold.reinsurance_summary (USD) + silver.pc_reinsurance_treaties (EUR). "
     "For REAL-TIME EVENT IMPACT: use gold.event_risk_correlation (NOAA × exposure) "
     "and gold.cat_event_pml (catastrophe PML). "
     "For LOSS DEVELOPMENT / RESERVING: use silver.pc_loss_development (AOY triangles)."),

    ("Joining Tables",
     "policy_customer_360 already joins dim_policy + dim_customer + dim_geography + dim_agent — "
     "prefer it over manual joins. "
     "claim_360 already joins fact_claim + dim_policy + dim_customer + dim_geography. "
     "When joining manually: "
     "  fact_claim.policy_id    = dim_policy.policy_id; "
     "  dim_policy.customer_id  = dim_customer.customer_id; "
     "  dim_policy.geo_id       = dim_geography.geo_id; "
     "  dim_policy.agent_id     = dim_agent.agent_id; "
     "  fact_premium.policy_id  = dim_policy.policy_id. "
     "For P&C silver: pc_claims.policy_id = pc_policies.policy_id."),

    ("Reporting Conventions",
     "Default to current calendar year unless the user supplies a date range. "
     "Filter by line_of_business when comparing across PL/CL/SP. "
     "Format monetary values with compact suffixes (K/M/B); allianz_silver.pc_* and "
     "allianz_gold.solvency_capital are EUR-denominated (column suffix _eur_m); the "
     "rest are USD. Display ratios as percentages with 1 decimal."),

    ("Data Freshness",
     "underwriting_kpis, book_health and loss_ratio_by_segment refresh on each "
     "DLT pipeline run. NOAA alerts, weather observations and catastrophe events "
     "refresh hourly via the allianz_external_feeds_hourly job (external feeds land "
     "in /Volumes/<catalog>/allianz_bronze/landing/external/ then merge into "
     "allianz_bronze tables before DLT processes them). The migrated pc_* reference "
     "data is a quarterly/annual snapshot."),

    ("FAQ — How do I…?",
     "Q: How do I see my worst performing segments? "
     "A: SELECT * FROM gold.loss_ratio_by_segment WHERE gross_premium_usd > 50000 "
     "ORDER BY combined_ratio DESC. "
     "Q: How do I see catastrophe exposure right now? "
     "A: SELECT * FROM gold.event_risk_correlation WHERE exposed_tiv_usd IS NOT NULL "
     "ORDER BY exposed_tiv_usd DESC. "
     "Q: How do I see my reserves and IBNR by accident year? "
     "A: SELECT accident_year, line_of_business, SUM(case_reserves_eur_m), "
     "SUM(ibnr_eur_m), SUM(ultimate_loss_eur_m) FROM silver.pc_loss_development "
     "GROUP BY accident_year, line_of_business. "
     "Q: How do I compare a sub-line's combined ratio to industry? "
     "A: SELECT sub_line, combined_ratio_pct FROM silver.pc_combined_ratios "
     "WHERE reporting_quarter = (SELECT MAX(reporting_quarter) FROM silver.pc_combined_ratios). "
     "Then compare against the benchmarks in the 'Industry Benchmarks' instruction."),
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
