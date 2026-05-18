"""
Create or update the Allianz Insurance Genie Space.

Strategy:
  1. Create the Genie space (data-room) via /api/2.0/data-rooms.
  2. Attach Unity Catalog tables via the GenieSpaceExport v2 serialized payload
     (the only programmatic route to set table references).
  3. Add general instructions (glossary, table guidance) via the
     /data-rooms/{id}/instructions endpoint.
  4. Add curated example questions via the /data-rooms/{id}/curated-questions
     endpoint.

Run:
    uv run scripts/build_genie.py
"""
from __future__ import annotations

import json
import subprocess
import sys

CATALOG = "serverless_stable_xhky6g_catalog"
SCHEMA = "allianz_gold"
WAREHOUSE = "cf18de10632b58c8"
PROFILE = "fe-vm-fevm-serverless-stable-xhky6g"
TITLE = "Allianz Insurance Intelligence — Genie"

TABLES = [
    ("underwriting_kpis",
     "Monthly underwriting KPIs by LOB: GWP, NWP, loss ratio, expense ratio, combined ratio."),
    ("claims_summary",
     "Aggregated claims by LOB/product/peril — count, total incurred, avg severity, cat & fraud counts."),
    ("exposure_accumulation",
     "TIV and policy count by state, CRESTA zone, region, LOB and product."),
    ("solvency_capital",
     "Simplified Solvency II view: SCR breakdown, MCR, own funds, solvency ratio, market value."),
    ("asset_portfolio_summary",
     "Investment portfolio by asset class and rating: market value, yield, duration, ESG."),
    ("event_risk_correlation",
     "Live NOAA alerts joined to per-state Allianz exposure (TIV and policy count)."),
    ("cat_event_pml",
     "Catastrophe events (GDACS) with peril severity factor and PML estimate by state."),
    ("dim_policy",
     "Policies across all LOBs with product, premium, sum insured, status."),
    ("dim_customer",
     "Customers — individuals and businesses with demographics and loyalty tier."),
    ("dim_geography",
     "Geography reference: state, region, ZIP, CRESTA zone, windstorm/flood zone."),
    ("fact_premium",
     "Monthly premium installments — gross premium, commission, tax, ceded, net premium."),
    ("fact_claim",
     "Claim fact with peril, loss/report dates, incurred, paid, salvage, fraud flag, cat code."),
    ("fact_exposure",
     "Per-policy exposure snapshot used for accumulation analysis."),
    ("dim_reinsurance_treaty",
     "Reinsurance treaties: type, reinsurer, limit, retention, cession %."),
    ("reinsurance_summary",
     "Reinsurance treaty summary by LOB and reinsurer (limit, ceded premium, avg cession)."),
    ("noaa_alerts",
     "Active NOAA weather alerts (type, severity, urgency, area)."),
    ("catastrophe_events",
     "GDACS catastrophe events (earthquake/cyclone/flood/wildfire) with lat/lon."),
    ("weather_observations",
     "Open-Meteo current weather per CRESTA city (temp, wind, precipitation, pressure)."),
]

INSTRUCTIONS = [
    ("Lines of Business",
     "Allianz operates Personal Lines (auto, home, life, renters, umbrella), Commercial Lines "
     "(property, GL, workers comp, D&O, professional liability), and Specialty (marine, aviation, "
     "cyber, environmental, energy). The dim_policy.line_of_business column stores values "
     "'Personal', 'Commercial', and 'Specialty'."),

    ("Glossary",
     "GWP = Gross Written Premium. NWP = Net Written Premium (after reinsurance ceded). "
     "TIV = Total Insured Value. SCR = Solvency Capital Requirement. MCR = Minimum Capital "
     "Requirement. Combined Ratio = Loss Ratio + Expense Ratio (<100% is profitable). "
     "PML = Probable Maximum Loss. CRESTA = Catastrophe Risk Evaluating and Standardizing "
     "Target Accumulation zones."),

    ("Table Guidance",
     "When users ask about 'risk' or 'solvency', use solvency_capital + asset_portfolio_summary. "
     "When users ask about 'exposure' or 'accumulation', use exposure_accumulation + dim_geography. "
     "For 'real-time' or 'live' event impact, use event_risk_correlation (NOAA alerts × exposure) "
     "or cat_event_pml (catastrophe PML estimates)."),

    ("Reporting Conventions",
     "Filter by line_of_business when comparing across PL/CL/SP. Report monetary values in USD "
     "with compact suffixes (K/M/B). Display ratios as percentages. Default to current year unless "
     "the user specifies a date range."),

    ("Data Freshness",
     "underwriting_kpis is materialized monthly per LOB. noaa_alerts, weather_observations, and "
     "catastrophe_events refresh hourly via the allianz_external_feeds_hourly job."),
]

CURATED_QUESTIONS = [
    "What is gross written premium by line of business this year?",
    "Show the combined ratio trend across Personal, Commercial, and Specialty lines over the last 12 months.",
    "Which states have the highest total insured value (TIV) accumulation?",
    "How many open cyber claims do we have and what's the average severity?",
    "What is our current solvency ratio and what drives the SCR breakdown?",
    "Show the asset portfolio split by class and credit rating.",
    "Which catastrophe events overlap with our exposure footprint right now?",
    "Top 10 perils by total incurred loss across the book.",
    "What is the claim count and total incurred loss by product?",
    "Show NOAA active alerts and the exposed TIV by event type.",
    "Which CRESTA zones have the highest aviation and marine exposure?",
    "How much premium has been ceded to reinsurance by treaty type?",
]


def run(args, **kw):
    res = subprocess.run(args, capture_output=True, text=True, **kw)
    if res.returncode != 0:
        print(" ".join(args))
        print("STDERR:", res.stderr)
        raise SystemExit(res.returncode)
    return res.stdout


def find_or_create_space() -> str:
    out = run(["databricks", "api", "get", "/api/2.0/data-rooms", "--profile", PROFILE])
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
    tables_sorted = sorted(TABLES, key=lambda x: f"{CATALOG}.{SCHEMA}.{x[0]}")
    export = {
        "version": 2,
        "data_sources": {
            "tables": [
                {"identifier": f"{CATALOG}.{SCHEMA}.{t}", "description": [d]}
                for t, d in tables_sorted
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


def add_instructions(space_id: str):
    existing_titles = {i.get("title") for i in list_existing(space_id, "instructions")}
    for title, content in INSTRUCTIONS:
        if title in existing_titles:
            continue
        run([
            "databricks", "api", "post",
            f"/api/2.0/data-rooms/{space_id}/instructions",
            "--json", json.dumps({"title": title, "content": content}),
            "--profile", PROFILE,
        ])
        print(f"  + instruction: {title}")


def add_curated_questions(space_id: str):
    existing = {q.get("question") for q in list_existing(space_id, "curated-questions")}
    for q in CURATED_QUESTIONS:
        if q in existing:
            continue
        run([
            "databricks", "api", "post",
            f"/api/2.0/data-rooms/{space_id}/curated-questions",
            "--json", json.dumps({"curated_question": {"question": q}}),
            "--profile", PROFILE,
        ])
        print(f"  + question: {q[:80]}")


def main():
    space_id = find_or_create_space()
    print(f"Genie space: {space_id}")

    print("Attaching tables…")
    attached = attach_tables(space_id)
    for t in attached:
        print(f"  • {t}")

    print("Adding general instructions…")
    add_instructions(space_id)

    print("Adding curated questions…")
    add_curated_questions(space_id)

    print()
    print(f"Genie URL: https://fevm-serverless-stable-xhky6g.cloud.databricks.com/genie/rooms/{space_id}")


if __name__ == "__main__":
    main()
