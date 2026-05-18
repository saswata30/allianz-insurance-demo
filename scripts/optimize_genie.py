"""
Optimize the Allianz Genie Space against the databricks-genie-workbench
IQ Scanner. Produces a full GSL v2 serialized_space payload that targets the
"Ready to Optimize" maturity tier (10/12 checks pass).

What this script writes
-----------------------
  • 12 gold marts (stays within Workbench's recommended 1–12 data sources)
  • Per-column descriptions + enable_entity_matching / enable_format_assistance
  • 5-section canonical text_instructions (PURPOSE / DISAMBIGUATION /
    DATA QUALITY NOTES / CONSTRAINTS / Instructions you must follow when
    providing summaries), <2000 chars, no SQL-in-prose
  • 3 join_specs (multi-table relationships)
  • 12 example_question_sqls with usage_guidance
  • 5 measures, 5 filters, 3 expressions (sql_snippets)
  • 10 benchmark question + expected_sql pairs

Optimization checks 11–12 require running the GSO Auto-Optimize workflow,
which needs Lakebase + MLflow Prompt Registry — out of scope here.

Run:
    uv run --no-project scripts/optimize_genie.py
    uv run --no-project scripts/optimize_genie.py --dry-run   # build + score only
"""
from __future__ import annotations

import argparse
import json
import secrets
import subprocess
import sys
from pathlib import Path

CATALOG = "serverless_stable_xhky6g_catalog"
GOLD = "allianz_gold"
WAREHOUSE = "cf18de10632b58c8"
PROFILE = "fe-vm-fevm-serverless-stable-xhky6g"
TITLE = "Allianz Insurance Intelligence — Genie"


def hex_id() -> str:
    return secrets.token_hex(16)


def fq(table: str) -> str:
    return f"{CATALOG}.{GOLD}.{table}"


# ---------------------------------------------------------------------------
# Column metadata for the 12 chosen tables.
# Each column is (column_name, description, kind)
# where kind ∈ {"entity"  → enable_entity_matching only,
#               "format"  → enable_format_assistance only,
#               "both"    → both enabled (default for string FK / categorical)}.
# ---------------------------------------------------------------------------
TABLES_SPEC = [
    {
        "identifier": fq("policy_customer_360"),
        "description": (
            "Policy 360: every Allianz P&C policy joined to its customer, "
            "geography and writing agent — one row per policy. Use for "
            "policy-centric questions, channel/segment analysis, agent and "
            "customer-tier mix."
        ),
        "columns": [
            ("policy_id", "Unique policy identifier (PK).", "entity"),
            ("line_of_business", "Personal / Commercial / Specialty.", "entity"),
            ("product", "Sub-line product e.g. Auto, Home, Cyber.", "entity"),
            ("lob_code", "2-letter LOB code PL/CL/SP.", "entity"),
            ("policy_status", "Active / Lapsed / Cancelled / Pending.", "entity"),
            ("channel", "Distribution channel: Agent/Broker/Direct/Digital/Aggregator.", "entity"),
            ("effective_date", "Policy effective date.", "format"),
            ("expiration_date", "Policy expiration date.", "format"),
            ("annual_premium_usd", "Annual premium written, USD.", "format"),
            ("sum_insured_usd", "Sum insured / total insured value, USD.", "format"),
            ("deductible_usd", "Per-claim deductible, USD.", "format"),
            ("reinsured", "True if policy is ceded to reinsurance.", "entity"),
            ("underwriter_score", "Underwriter quality score 0-100.", "format"),
            ("customer_id", "Customer PK.", "entity"),
            ("customer_type", "Individual / Business.", "entity"),
            ("loyalty_tier", "Bronze / Silver / Gold / Platinum.", "entity"),
            ("income_bracket", "Customer income bracket.", "entity"),
            ("credit_score", "Customer credit score.", "format"),
            ("state_code", "US two-letter state code.", "entity"),
            ("region", "US Census region (Northeast/South/Midwest/West).", "entity"),
            ("cresta_zone", "CRESTA accumulation zone.", "entity"),
            ("flood_zone", "FEMA flood zone X/A/AE/V.", "entity"),
            ("agent_channel", "Channel of the writing agent.", "entity"),
            ("agent_specialty_lob", "Agent's specialty line of business.", "entity"),
        ],
    },
    {
        "identifier": fq("claim_360"),
        "description": (
            "Claim 360: every claim joined to its policy, customer and "
            "geography. Use for claim frequency, severity, peril, fraud, "
            "report-lag and catastrophe analysis."
        ),
        "columns": [
            ("claim_id", "Unique claim identifier (PK).", "entity"),
            ("policy_id", "Policy this claim was made against (FK).", "entity"),
            ("line_of_business", "Personal / Commercial / Specialty.", "entity"),
            ("product", "Sub-line product.", "entity"),
            ("peril", "Insured peril (e.g. Fire, Wind, Theft, Cyber).", "entity"),
            ("claim_status", "Open / Approved / Denied / Closed / Litigation.", "entity"),
            ("loss_date", "Date of loss.", "format"),
            ("report_date", "Date the claim was reported.", "format"),
            ("report_lag_days", "Days between loss and report (claim ops KPI).", "format"),
            ("incurred_amount_usd", "Incurred loss amount, USD.", "format"),
            ("paid_amount_usd", "Paid loss amount, USD.", "format"),
            ("reserve_amount_usd", "Case reserve held, USD.", "format"),
            ("net_paid_usd", "Paid minus salvage and subrogation, USD.", "format"),
            ("fraud_flag", "True if fraud is suspected.", "entity"),
            ("catastrophe_code", "Cat code if claim is part of a designated catastrophe.", "entity"),
            ("is_cat", "True if this claim is part of a catastrophe event.", "entity"),
            ("severity_pct_of_sum_insured", "Incurred / sum insured ratio (0-1).", "format"),
            ("customer_type", "Individual / Business.", "entity"),
            ("loyalty_tier", "Bronze / Silver / Gold / Platinum.", "entity"),
            ("state_code", "Two-letter US state code.", "entity"),
            ("region", "US Census region.", "entity"),
            ("cresta_zone", "CRESTA accumulation zone.", "entity"),
        ],
    },
    {
        "identifier": fq("underwriting_kpis"),
        "description": (
            "Monthly underwriting KPIs by line of business: GWP, NWP, "
            "incurred loss, loss ratio, expense ratio and combined ratio."
        ),
        "columns": [
            ("month", "Month bucket (first day of month).", "format"),
            ("line_of_business", "Personal / Commercial / Specialty.", "entity"),
            ("gross_written_premium_usd", "Gross written premium for the month, USD.", "format"),
            ("net_written_premium_usd", "Net written premium (after reinsurance), USD.", "format"),
            ("commission_usd", "Commission paid, USD.", "format"),
            ("ceded_premium_usd", "Premium ceded to reinsurers, USD.", "format"),
            ("incurred_loss_usd", "Incurred losses, USD.", "format"),
            ("paid_loss_usd", "Paid losses, USD.", "format"),
            ("claim_count", "Number of claims booked in the month.", "format"),
            ("loss_ratio", "Incurred loss / GWP (0-1).", "format"),
            ("expense_ratio", "Commission / GWP (0-1).", "format"),
            ("combined_ratio", "Loss ratio + expense ratio (0-1). <1 = underwriting profit.", "format"),
        ],
    },
    {
        "identifier": fq("book_health"),
        "description": (
            "Portfolio book health by LOB and product: policy count, "
            "inforce premium and TIV, frequency per 100 policies, severity, "
            "loss ratio and cat/fraud counts."
        ),
        "columns": [
            ("line_of_business", "Personal / Commercial / Specialty.", "entity"),
            ("product", "Sub-line product.", "entity"),
            ("active_policy_count", "Active in-force policies.", "format"),
            ("inforce_premium_usd", "In-force annual premium, USD.", "format"),
            ("inforce_sum_insured_usd", "In-force sum insured (TIV), USD.", "format"),
            ("avg_underwriter_score", "Average underwriter quality score.", "format"),
            ("claim_count", "Total claims for this LOB/product.", "format"),
            ("incurred_loss_usd", "Total incurred loss, USD.", "format"),
            ("avg_severity_usd", "Average claim severity, USD.", "format"),
            ("cat_claim_count", "Claims tagged with a catastrophe code.", "format"),
            ("fraud_claim_count", "Claims with fraud_flag = true.", "format"),
            ("claim_frequency_per_100", "Claims per 100 active policies.", "format"),
            ("loss_ratio", "Incurred loss / inforce premium (0-1).", "format"),
            ("avg_premium_per_policy", "Average annual premium per policy, USD.", "format"),
        ],
    },
    {
        "identifier": fq("peril_loss_summary"),
        "description": (
            "Loss summary by peril across the entire P&C book: claim "
            "frequency, average and p95 severity, catastrophe share of loss."
        ),
        "columns": [
            ("peril", "Insured peril.", "entity"),
            ("claim_count", "Number of claims for this peril.", "format"),
            ("total_incurred_usd", "Total incurred loss for this peril, USD.", "format"),
            ("avg_severity_usd", "Average claim severity, USD.", "format"),
            ("p95_severity_usd", "95th-percentile claim severity, USD.", "format"),
            ("cat_claim_count", "Claims from this peril tagged as catastrophe.", "format"),
            ("cat_incurred_usd", "Incurred loss from cat claims, USD.", "format"),
            ("cat_share_of_loss", "Cat incurred / total incurred (0-1).", "format"),
            ("distinct_lobs", "Number of distinct LOBs that wrote this peril.", "format"),
        ],
    },
    {
        "identifier": fq("loss_ratio_by_segment"),
        "description": (
            "Loss ratio drilled to LOB × product × state × channel × "
            "customer_type. Use for portfolio profitability deep-dives and "
            "rate-action candidate identification."
        ),
        "columns": [
            ("line_of_business", "Personal / Commercial / Specialty.", "entity"),
            ("product", "Sub-line product.", "entity"),
            ("state_code", "US state code.", "entity"),
            ("channel", "Distribution channel.", "entity"),
            ("customer_type", "Individual / Business.", "entity"),
            ("gross_premium_usd", "GWP for the segment, USD.", "format"),
            ("net_premium_usd", "NWP for the segment, USD.", "format"),
            ("commission_usd", "Commission for the segment, USD.", "format"),
            ("incurred_loss_usd", "Incurred loss for the segment, USD.", "format"),
            ("claim_count", "Claims booked in the segment.", "format"),
            ("loss_ratio", "Segment loss ratio (0-1).", "format"),
            ("expense_ratio", "Segment expense ratio (0-1).", "format"),
            ("combined_ratio", "Segment combined ratio (0-1).", "format"),
        ],
    },
    {
        "identifier": fq("exposure_accumulation"),
        "description": (
            "Total insured value (TIV) and policy count aggregated by "
            "state, CRESTA zone, region, LOB and product."
        ),
        "columns": [
            ("state_code", "US state code.", "entity"),
            ("cresta_zone", "CRESTA zone identifier.", "entity"),
            ("region", "US Census region.", "entity"),
            ("line_of_business", "Personal / Commercial / Specialty.", "entity"),
            ("product", "Sub-line product.", "entity"),
            ("total_insured_value_usd", "Sum insured aggregated, USD.", "format"),
            ("annual_premium_usd", "Annual premium aggregated, USD.", "format"),
            ("policy_count", "Number of policies in the slice.", "format"),
        ],
    },
    {
        "identifier": fq("solvency_capital"),
        "description": (
            "Simplified Solvency II view: SCR components (underwriting, "
            "market, counterparty, operational), MCR, own funds, solvency "
            "ratio, market value. Tall format — one row per metric."
        ),
        "columns": [
            ("metric", "Solvency metric name (e.g. 'SCR (Total)', 'Own Funds', 'Solvency Ratio').", "entity"),
            ("value_usd", "Value of the metric, USD (Solvency Ratio is a fraction).", "format"),
        ],
    },
    {
        "identifier": fq("asset_portfolio_summary"),
        "description": (
            "Investment portfolio aggregated by asset class and rating: "
            "market value, book value, average yield, duration, ESG score."
        ),
        "columns": [
            ("asset_class", "Govt Bond / Corp Bond IG / Corp Bond HY / Equity / Real Estate / MMF / Mortgage.", "entity"),
            ("rating", "Credit rating (AAA…CCC/NotRated).", "entity"),
            ("market_value_usd", "Total market value, USD.", "format"),
            ("book_value_usd", "Total book value, USD.", "format"),
            ("avg_yield_pct", "Weighted average yield (0-1).", "format"),
            ("avg_duration_yrs", "Average modified duration in years.", "format"),
            ("avg_esg_score", "Average ESG score (0-100).", "format"),
            ("position_count", "Number of positions in the slice.", "format"),
        ],
    },
    {
        "identifier": fq("reinsurance_summary"),
        "description": (
            "Reinsurance treaty summary by line of business, reinsurer and "
            "treaty type: total limit, premium ceded, average cession."
        ),
        "columns": [
            ("line_of_business", "Personal / Commercial / Specialty.", "entity"),
            ("reinsurer", "Reinsurer (Munich Re, Swiss Re, Hannover Re, …).", "entity"),
            ("treaty_type", "QuotaShare / SurplusShare / ExcessOfLoss / Stop-Loss / CatastropheXL.", "entity"),
            ("total_limit_usd", "Aggregated treaty limit, USD.", "format"),
            ("premium_ceded_usd", "Premium ceded to this reinsurer/treaty, USD.", "format"),
            ("avg_cession_pct", "Average cession percentage (0-1).", "format"),
            ("treaty_count", "Number of treaties in the slice.", "format"),
        ],
    },
    {
        "identifier": fq("event_risk_correlation"),
        "description": (
            "Live NOAA active weather alerts joined to per-state Allianz "
            "exposure. Use for real-time portfolio-impact questions."
        ),
        "columns": [
            ("alert_id", "NOAA alert ID (PK).", "entity"),
            ("event_type", "NOAA event type (e.g. Tornado Warning, Flash Flood Warning).", "entity"),
            ("severity", "Severity label (Extreme/Severe/Moderate/Minor/Unknown).", "entity"),
            ("urgency", "Urgency label (Immediate/Expected/Future/Past/Unknown).", "entity"),
            ("effective_utc", "When the alert becomes effective (UTC).", "format"),
            ("expires_utc", "When the alert expires (UTC).", "format"),
            ("state_code", "Affected US state code.", "entity"),
            ("state_name", "Affected US state name.", "entity"),
            ("exposed_tiv_usd", "Allianz TIV exposed to this alert, USD.", "format"),
            ("exposed_policy_count", "Allianz policies exposed to this alert.", "format"),
        ],
    },
    {
        "identifier": fq("cat_event_pml"),
        "description": (
            "GDACS catastrophe events with peril severity factor and PML "
            "(Probable Maximum Loss) estimate by state."
        ),
        "columns": [
            ("event_id", "Catastrophe event ID (PK).", "entity"),
            ("title", "Event title.", "entity"),
            ("event_type", "Earthquake / TropicalCyclone / Flood / Wildfire / Drought / Volcanic / Other.", "entity"),
            ("severity", "GDACS severity (low / moderate / high).", "entity"),
            ("event_time_utc", "Event timestamp (UTC).", "format"),
            ("latitude", "Event latitude.", "format"),
            ("longitude", "Event longitude.", "format"),
            ("state_code", "Affected US state code.", "entity"),
            ("state_tiv_usd", "State-level TIV at risk, USD.", "format"),
            ("severity_factor", "Peril-specific severity factor (0-1).", "format"),
            ("pml_estimate_usd", "Probable maximum loss estimate, USD.", "format"),
        ],
    },
]


# ---------------------------------------------------------------------------
# 5-section text instructions (≤2000 chars, no SQL-in-prose).
# ---------------------------------------------------------------------------
TEXT_INSTRUCTIONS_BLOCK = """## PURPOSE
- Answer Property & Casualty insurance questions across Personal, Commercial and Specialty lines for Allianz.
- Audience: actuaries, underwriters, finance, risk and reinsurance teams. Assume insurance fluency.

## DISAMBIGUATION
- "P&C" or "Property and Casualty" means all three lines combined (Personal + Commercial + Specialty).
- "This year" and "YTD" default to the current calendar year unless the user supplies a date range.
- "Q1/Q2/Q3/Q4" mean calendar quarters unless the user explicitly says "fiscal".
- "Top" defaults to top 10 unless the user states otherwise.

## DATA QUALITY NOTES
- claim_360.is_cat is true only when catastrophe_code is set on the claim.
- solvency_capital is a simplified illustrative view; for regulatory reporting use the EUR series in the underlying Allianz P&C source.
- event_risk_correlation only contains alerts that touch Allianz states. Rows without exposed_tiv_usd indicate alerts outside the footprint.
- Combined ratio < 1 is an underwriting profit. US industry average is roughly 1.01 in 2024.

## CONSTRAINTS
- Never project raw customer_name without an aggregation.
- Default monetary values to compact suffixes (K, Musd, Busd).
- Always group or filter by line_of_business when comparing across PL, CL and SP.

## Instructions you must follow when providing summaries
- Round percentages to one decimal place.
- Always state the currency and the date range used in the summary.
- When reporting a loss or combined ratio, also state the relevant industry benchmark and whether the value beats or trails it.
"""


# ---------------------------------------------------------------------------
# Example question + SQL pairs (12).
# Each one is a teaching example showing how to answer a common P&C question.
# ---------------------------------------------------------------------------
EXAMPLE_SQLS = [
    {
        "question": "What is gross written premium by line of business this year?",
        "sql": (
            "SELECT line_of_business, "
            "ROUND(SUM(gross_written_premium_usd) / 1e6, 1) AS gwp_musd "
            f"FROM {fq('underwriting_kpis')} "
            "WHERE month >= date_trunc('year', current_date()) "
            "GROUP BY line_of_business "
            "ORDER BY gwp_musd DESC"
        ),
        "usage_guidance": "Use whenever the user asks for premium volume by LOB; "
                          "swap month filter for the requested date range.",
    },
    {
        "question": "Show combined ratio trend by line of business for the last 12 months.",
        "sql": (
            "SELECT date_trunc('MONTH', month) AS m, line_of_business, "
            "ROUND(AVG(combined_ratio) * 100, 1) AS combined_ratio_pct "
            f"FROM {fq('underwriting_kpis')} "
            "WHERE month >= add_months(current_date(), -12) "
            "GROUP BY 1, 2 ORDER BY 1, 2"
        ),
        "usage_guidance": "Use for any combined-ratio or loss-ratio trend question; "
                          "swap the time window as needed.",
    },
    {
        "question": "What are the top 10 perils by total incurred loss?",
        "sql": (
            "SELECT peril, claim_count, "
            "ROUND(total_incurred_usd / 1e6, 1) AS incurred_musd, "
            "ROUND(p95_severity_usd, 0) AS p95_severity_usd, "
            "ROUND(cat_share_of_loss * 100, 1) AS cat_share_pct "
            f"FROM {fq('peril_loss_summary')} "
            "ORDER BY total_incurred_usd DESC LIMIT 10"
        ),
        "usage_guidance": "Use for any 'top N perils' or peril-severity question.",
    },
    {
        "question": "Show book health (frequency and severity) by product for Personal Lines.",
        "sql": (
            "SELECT product, active_policy_count, "
            "ROUND(claim_frequency_per_100, 2) AS freq_per_100, "
            "ROUND(avg_severity_usd, 0) AS avg_severity_usd, "
            "ROUND(loss_ratio * 100, 1) AS loss_ratio_pct "
            f"FROM {fq('book_health')} "
            "WHERE line_of_business = 'Personal' "
            "ORDER BY active_policy_count DESC"
        ),
        "usage_guidance": "Use when the user asks for frequency / severity / loss-ratio by product within a LOB.",
    },
    {
        "question": "Which segments have a combined ratio above 100%?",
        "sql": (
            "SELECT line_of_business, product, state_code, channel, customer_type, "
            "ROUND(combined_ratio * 100, 1) AS combined_ratio_pct, "
            "ROUND(gross_premium_usd / 1e6, 1) AS gwp_musd "
            f"FROM {fq('loss_ratio_by_segment')} "
            "WHERE combined_ratio > 1 AND gross_premium_usd > 50000 "
            "ORDER BY combined_ratio DESC LIMIT 20"
        ),
        "usage_guidance": "Use whenever the user asks for under-performing segments or rate-action candidates.",
    },
    {
        "question": "What is our total insured value (TIV) by US state?",
        "sql": (
            "SELECT state_code, "
            "ROUND(SUM(total_insured_value_usd) / 1e9, 2) AS tiv_busd, "
            "SUM(policy_count) AS policies "
            f"FROM {fq('exposure_accumulation')} "
            "GROUP BY state_code ORDER BY tiv_busd DESC"
        ),
        "usage_guidance": "Use for any exposure-accumulation question by geography.",
    },
    {
        "question": "Show our solvency capital breakdown (SCR by component, own funds, ratio).",
        "sql": (
            "SELECT metric, ROUND(value_usd, 2) AS value_usd "
            f"FROM {fq('solvency_capital')} ORDER BY value_usd DESC"
        ),
        "usage_guidance": "Use for any solvency / SCR / own funds question.",
    },
    {
        "question": "Asset portfolio breakdown by asset class and rating.",
        "sql": (
            "SELECT asset_class, rating, "
            "ROUND(SUM(market_value_usd) / 1e6, 1) AS mv_musd, "
            "ROUND(AVG(avg_yield_pct) * 100, 2) AS avg_yield_pct, "
            "ROUND(AVG(avg_duration_yrs), 2) AS avg_duration_yrs "
            f"FROM {fq('asset_portfolio_summary')} "
            "GROUP BY asset_class, rating ORDER BY mv_musd DESC"
        ),
        "usage_guidance": "Use for any asset-portfolio question (class / rating / yield / duration / ESG).",
    },
    {
        "question": "How much premium have we ceded to each reinsurer this year?",
        "sql": (
            "SELECT reinsurer, treaty_type, "
            "ROUND(SUM(premium_ceded_usd) / 1e6, 1) AS ceded_musd, "
            "ROUND(AVG(avg_cession_pct) * 100, 1) AS avg_cession_pct, "
            "SUM(treaty_count) AS treaty_count "
            f"FROM {fq('reinsurance_summary')} "
            "GROUP BY reinsurer, treaty_type ORDER BY ceded_musd DESC"
        ),
        "usage_guidance": "Use for any reinsurance ceded-premium / treaty-utilization question.",
    },
    {
        "question": "Show active NOAA alerts and the Allianz TIV they expose.",
        "sql": (
            "SELECT event_type, severity, urgency, state_code, state_name, "
            "ROUND(exposed_tiv_usd / 1e9, 2) AS exposed_tiv_busd, "
            "exposed_policy_count, effective_utc, expires_utc "
            f"FROM {fq('event_risk_correlation')} "
            "WHERE exposed_tiv_usd IS NOT NULL "
            "ORDER BY exposed_tiv_usd DESC LIMIT 20"
        ),
        "usage_guidance": "Use for any 'what events are hitting our book right now' question.",
    },
    {
        "question": "What is the estimated PML for current catastrophe events?",
        "sql": (
            "SELECT event_type, severity, title, state_code, "
            "ROUND(state_tiv_usd / 1e9, 2) AS state_tiv_busd, "
            "ROUND(severity_factor * 100, 1) AS severity_factor_pct, "
            "ROUND(pml_estimate_usd / 1e6, 1) AS pml_musd "
            f"FROM {fq('cat_event_pml')} "
            "WHERE pml_estimate_usd IS NOT NULL "
            "ORDER BY pml_estimate_usd DESC LIMIT 20"
        ),
        "usage_guidance": "Use for any catastrophe PML or 'how bad could this get' question.",
    },
    {
        "question": "Top 20 claims by report lag, with state and peril.",
        "sql": (
            "SELECT claim_id, line_of_business, product, peril, state_code, "
            "report_lag_days, ROUND(incurred_amount_usd / 1e3, 1) AS incurred_kusd, "
            "claim_status, loss_date, report_date "
            f"FROM {fq('claim_360')} "
            "ORDER BY report_lag_days DESC LIMIT 20"
        ),
        "usage_guidance": "Use for claims-operations questions about reporting latency.",
    },
]


# ---------------------------------------------------------------------------
# SQL snippets: 5 measures, 5 filters, 3 expressions
# ---------------------------------------------------------------------------
MEASURES = [
    {"alias": "total_gwp_musd",
     "sql": f"ROUND(SUM(`{fq('underwriting_kpis')}`.`gross_written_premium_usd`) / 1e6, 1)",
     "display_name": "Total GWP ($M)"},
    {"alias": "total_incurred_musd",
     "sql": f"ROUND(SUM(`{fq('underwriting_kpis')}`.`incurred_loss_usd`) / 1e6, 1)",
     "display_name": "Total Incurred Loss ($M)"},
    {"alias": "avg_combined_ratio_pct",
     "sql": f"ROUND(AVG(`{fq('underwriting_kpis')}`.`combined_ratio`) * 100, 1)",
     "display_name": "Combined Ratio %"},
    {"alias": "total_claim_count",
     "sql": f"SUM(`{fq('peril_loss_summary')}`.`claim_count`)",
     "display_name": "Total Claims"},
    {"alias": "total_tiv_busd",
     "sql": f"ROUND(SUM(`{fq('exposure_accumulation')}`.`total_insured_value_usd`) / 1e9, 2)",
     "display_name": "Total Insured Value ($B)"},
]

FILTERS = [
    {"display_name": "Current calendar year",
     "sql": f"`{fq('underwriting_kpis')}`.`month` >= date_trunc('year', current_date())"},
    {"display_name": "Last 12 months",
     "sql": f"`{fq('underwriting_kpis')}`.`month` >= add_months(current_date(), -12)"},
    {"display_name": "Active policies only",
     "sql": f"`{fq('policy_customer_360')}`.`policy_status` = 'Active'"},
    {"display_name": "Catastrophe claims only",
     "sql": f"`{fq('claim_360')}`.`is_cat` = true"},
    {"display_name": "Personal Lines only",
     "sql": f"`{fq('policy_customer_360')}`.`line_of_business` = 'Personal'"},
]

EXPRESSIONS = [
    {"alias": "policy_year",
     "sql": f"YEAR(`{fq('policy_customer_360')}`.`effective_date`)",
     "display_name": "Policy Year"},
    {"alias": "loss_ratio_pct",
     "sql": f"ROUND(`{fq('underwriting_kpis')}`.`loss_ratio` * 100, 1)",
     "display_name": "Loss Ratio %"},
    {"alias": "severity_pct",
     "sql": f"ROUND(`{fq('claim_360')}`.`severity_pct_of_sum_insured` * 100, 1)",
     "display_name": "Severity % of Sum Insured"},
]


# ---------------------------------------------------------------------------
# Join specifications (3) — declare relationships between the gold marts.
# ---------------------------------------------------------------------------
JOIN_SPECS = [
    {
        "left_table": fq("claim_360"),
        "left_column": "policy_id",
        "right_table": fq("policy_customer_360"),
        "right_column": "policy_id",
        "relationship": "MANY_TO_ONE",
    },
    {
        "left_table": fq("event_risk_correlation"),
        "left_column": "state_code",
        "right_table": fq("exposure_accumulation"),
        "right_column": "state_code",
        "relationship": "MANY_TO_MANY",
    },
    {
        "left_table": fq("cat_event_pml"),
        "left_column": "state_code",
        "right_table": fq("exposure_accumulation"),
        "right_column": "state_code",
        "relationship": "MANY_TO_MANY",
    },
]


# ---------------------------------------------------------------------------
# Benchmark questions: 10 ground-truth question + expected SQL pairs.
# Hardcoded literal values only, no parameters.
# ---------------------------------------------------------------------------
BENCHMARKS = [
    ("How many active policies do we have?",
     f"SELECT SUM(active_policy_count) FROM {fq('book_health')}"),
    ("What is the combined ratio for Personal Lines in the last 12 months?",
     f"SELECT ROUND(AVG(combined_ratio) * 100, 1) AS combined_ratio_pct "
     f"FROM {fq('underwriting_kpis')} "
     f"WHERE line_of_business = 'Personal' AND month >= add_months(current_date(), -12)"),
    ("Top 5 states by total insured value.",
     f"SELECT state_code, ROUND(SUM(total_insured_value_usd) / 1e9, 2) AS tiv_busd "
     f"FROM {fq('exposure_accumulation')} GROUP BY state_code ORDER BY tiv_busd DESC LIMIT 5"),
    ("How many claims are tagged as catastrophe?",
     f"SELECT COUNT(*) FROM {fq('claim_360')} WHERE is_cat = true"),
    ("What is the average severity of cyber claims?",
     f"SELECT ROUND(AVG(incurred_amount_usd), 0) AS avg_severity_usd "
     f"FROM {fq('claim_360')} WHERE product = 'Cyber'"),
    ("Show our solvency ratio.",
     f"SELECT value_usd AS solvency_ratio FROM {fq('solvency_capital')} "
     f"WHERE metric = 'Solvency Ratio'"),
    ("Total premium ceded to Munich Re.",
     f"SELECT ROUND(SUM(premium_ceded_usd) / 1e6, 1) AS ceded_musd "
     f"FROM {fq('reinsurance_summary')} WHERE reinsurer = 'Munich Re'"),
    ("What is the average yield of AAA-rated assets?",
     f"SELECT ROUND(AVG(avg_yield_pct) * 100, 2) AS avg_yield_pct "
     f"FROM {fq('asset_portfolio_summary')} WHERE rating = 'AAA'"),
    ("How many active NOAA alerts affect our book?",
     f"SELECT COUNT(*) FROM {fq('event_risk_correlation')} "
     f"WHERE exposed_tiv_usd IS NOT NULL"),
    ("Top 5 perils by p95 severity.",
     f"SELECT peril, ROUND(p95_severity_usd, 0) AS p95_severity_usd "
     f"FROM {fq('peril_loss_summary')} ORDER BY p95_severity_usd DESC LIMIT 5"),
]


# ---------------------------------------------------------------------------
# Assembly — produce the GSL v2 payload (same shape Workbench's create_agent emits).
# ---------------------------------------------------------------------------
def split_sql(sql: str) -> list[str]:
    """Split a SQL string into 1-line array form expected by GSL."""
    return [sql.strip()]


def assemble_space() -> dict:
    # Data sources
    ds_tables = []
    for t in TABLES_SPEC:
        cc_list = []
        for col_name, col_desc, kind in t["columns"]:
            entry = {
                "column_name": col_name,
                "description": [col_desc],
            }
            if kind == "entity":
                entry["enable_entity_matching"] = True
                entry["enable_format_assistance"] = False
            elif kind == "format":
                entry["enable_entity_matching"] = False
                entry["enable_format_assistance"] = True
            else:
                entry["enable_entity_matching"] = True
                entry["enable_format_assistance"] = True
            cc_list.append(entry)
        cc_list.sort(key=lambda x: x["column_name"])
        ds_tables.append({
            "identifier": t["identifier"],
            "description": [t["description"]],
            "column_configs": cc_list,
        })
    ds_tables.sort(key=lambda x: x["identifier"])

    # Text instructions
    text_block = TEXT_INSTRUCTIONS_BLOCK
    content_lines = [l + "\n" for l in text_block.splitlines()]
    text_instructions = [{
        "id": hex_id(),
        "content": content_lines,
    }]

    # Example question SQLs
    eq_items = []
    for eq in EXAMPLE_SQLS:
        eq_items.append({
            "id": hex_id(),
            "question": [eq["question"]],
            "sql": split_sql(eq["sql"]),
            "usage_guidance": [eq["usage_guidance"]],
        })
    eq_items.sort(key=lambda x: x["id"])

    # SQL snippets
    snippets = {}
    snippets["measures"] = sorted(
        [{"id": hex_id(), "alias": m["alias"], "sql": [m["sql"]],
          "display_name": m["display_name"]} for m in MEASURES],
        key=lambda x: x["id"],
    )
    snippets["filters"] = sorted(
        [{"id": hex_id(), "display_name": f["display_name"], "sql": [f["sql"]]}
         for f in FILTERS],
        key=lambda x: x["id"],
    )
    snippets["expressions"] = sorted(
        [{"id": hex_id(), "alias": e["alias"], "sql": [e["sql"]],
          "display_name": e["display_name"]} for e in EXPRESSIONS],
        key=lambda x: x["id"],
    )

    # Join specs
    js_items = []
    for js in JOIN_SPECS:
        la = js["left_table"].split(".")[-1]
        ra = js["right_table"].split(".")[-1]
        sql = f"`{la}`.`{js['left_column']}` = `{ra}`.`{js['right_column']}`"
        rt = f"--rt=FROM_RELATIONSHIP_TYPE_{js['relationship']}--"
        js_items.append({
            "id": hex_id(),
            "left": {"identifier": js["left_table"], "alias": la},
            "right": {"identifier": js["right_table"], "alias": ra},
            "sql": [sql, rt],
        })
    js_items.sort(key=lambda x: x["id"])

    # Benchmarks
    bench_items = []
    for q, sql in BENCHMARKS:
        bench_items.append({
            "id": hex_id(),
            "question": [q],
            "answer": [{"format": "SQL", "content": split_sql(sql)}],
        })
    bench_items.sort(key=lambda x: x["id"])

    return {
        "version": 2,
        "data_sources": {"tables": ds_tables},
        "instructions": {
            "text_instructions": text_instructions,
            "example_question_sqls": eq_items,
            "sql_snippets": snippets,
            "join_specs": js_items,
        },
        "benchmarks": {"questions": bench_items},
    }


# ---------------------------------------------------------------------------
# Local IQ score check using the vendored workbench scoring module.
# ---------------------------------------------------------------------------
def local_iq_score(space: dict) -> dict:
    sys.path.insert(0, str(Path(__file__).parent))
    from genie_workbench import calculate_score
    return calculate_score(space, optimization_run=None)


def print_score(report: dict):
    print(f"  Score: {report['score']}/{report['total']}  "
          f"({report['maturity']})")
    print()
    for c in report["checks"]:
        flag = "✓" if c["passed"] else "✗"
        sev = c["severity"]
        sev_tag = "" if sev == "pass" else f" [{sev}]"
        print(f"   {flag} {c['label']:48s}{sev_tag}")
        if c.get("detail"):
            print(f"      └─ {c['detail']}")
    if report.get("warnings"):
        print()
        print("  Warnings:")
        for w in report["warnings"]:
            print(f"   ⚠ {w}")


# ---------------------------------------------------------------------------
# Push helpers
# ---------------------------------------------------------------------------
def run(args, **kw):
    res = subprocess.run(args, capture_output=True, text=True, **kw)
    if res.returncode != 0:
        print("STDERR:", res.stderr[:500])
        raise SystemExit(res.returncode)
    return res.stdout


def find_space() -> str:
    out = run(["databricks", "api", "get", "/api/2.0/data-rooms",
               "--profile", PROFILE])
    rooms = json.loads(out or "{}").get("data_rooms", [])
    for r in rooms:
        if r.get("display_name") == TITLE and r.get("lifecycle_state", "ACTIVE") != "TRASHED":
            return r["space_id"]
    raise SystemExit(f"Genie space '{TITLE}' not found.")


def push_space(space_id: str, payload: dict):
    serialized = json.dumps(payload)
    out = run([
        "databricks", "genie", "update-space", space_id,
        "--serialized-space", serialized,
        "--title", TITLE,
        "--profile", PROFILE,
    ])
    return json.loads(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Build and score locally; do not push to the workspace.")
    args = parser.parse_args()

    print("Building optimized Genie space…")
    space = assemble_space()
    Path("genie").mkdir(exist_ok=True)
    Path("genie/optimized_space.json").write_text(json.dumps(space, indent=2))
    print("  → wrote genie/optimized_space.json")

    print()
    print("Local IQ score:")
    score = local_iq_score(space)
    print_score(score)

    if args.dry_run:
        print()
        print("--dry-run: not pushing.")
        return

    space_id = find_space()
    print()
    print(f"Pushing to Genie space {space_id}…")
    body = push_space(space_id, space)
    parsed = json.loads(body["serialized_space"])
    print(f"  attached tables       : {len(parsed.get('data_sources', {}).get('tables', []))}")
    print(f"  text_instructions     : {len(parsed.get('instructions', {}).get('text_instructions', []))}")
    print(f"  example_question_sqls : {len(parsed.get('instructions', {}).get('example_question_sqls', []))}")
    print(f"  join_specs            : {len(parsed.get('instructions', {}).get('join_specs', []))}")
    snip = parsed.get("instructions", {}).get("sql_snippets", {})
    print(f"  sql_snippets          : "
          f"{len(snip.get('measures', []))} measures, "
          f"{len(snip.get('filters', []))} filters, "
          f"{len(snip.get('expressions', []))} expressions")
    print(f"  benchmarks            : {len(parsed.get('benchmarks', {}).get('questions', []))}")
    print()
    print(f"Genie URL: https://fevm-serverless-stable-xhky6g.cloud.databricks.com/"
          f"genie/rooms/{space_id}")


if __name__ == "__main__":
    main()
