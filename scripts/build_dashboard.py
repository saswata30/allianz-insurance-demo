"""
Build & deploy the Allianz Insurance Intelligence Lakeview dashboard.

Run:
    uv run --with httpx scripts/build_dashboard.py
"""
from __future__ import annotations

import json
import os
import subprocess
import uuid
from pathlib import Path

CATALOG = "serverless_stable_xhky6g_catalog"
SCHEMA = "allianz_gold"   # business marts
SILVER = "allianz_silver" # conformed dims/facts (for detail tables)
WAREHOUSE = "cf18de10632b58c8"
PROFILE = "fe-vm-fevm-serverless-stable-xhky6g"

ALLIANZ_BLUE = "#003781"
ALLIANZ_LIGHT = "#006192"
ALLIANZ_SKY = "#96DCFA"
ALLIANZ_ACCENT = "#FFAB00"
COLORS = [ALLIANZ_BLUE, ALLIANZ_LIGHT, ALLIANZ_SKY, ALLIANZ_ACCENT,
          "#00A972", "#FF3621", "#8BCAE7", "#AB4057"]


def uid(prefix=""):
    return f"{prefix}{uuid.uuid4().hex[:10]}"


# ---------------------------------------------------------------------------
# Datasets — one per consumption pattern, fully qualified, tested separately
# ---------------------------------------------------------------------------
DATASETS = [
    ("uw_kpis", "Underwriting KPIs",
     f"SELECT month, line_of_business, gross_written_premium_usd, "
     f"net_written_premium_usd, incurred_loss_usd, claim_count, "
     f"loss_ratio, expense_ratio, combined_ratio "
     f"FROM {CATALOG}.{SCHEMA}.underwriting_kpis"),

    ("kpi_totals", "KPI Totals",
     f"SELECT "
     f"  SUM(gross_written_premium_usd) AS total_gwp, "
     f"  SUM(net_written_premium_usd)   AS total_nwp, "
     f"  SUM(incurred_loss_usd)         AS total_losses, "
     f"  SUM(claim_count)               AS total_claims, "
     f"  CASE WHEN SUM(gross_written_premium_usd) > 0 "
     f"       THEN SUM(incurred_loss_usd) / SUM(gross_written_premium_usd) END AS overall_loss_ratio "
     f"FROM {CATALOG}.{SCHEMA}.underwriting_kpis"),

    ("claims_summary", "Claims Summary",
     f"SELECT line_of_business, product, peril, claim_count, total_incurred_usd, "
     f"avg_severity_usd, max_severity_usd, cat_claim_count, fraud_flag_count "
     f"FROM {CATALOG}.{SCHEMA}.claims_summary"),

    ("exposure", "Exposure Accumulation",
     f"SELECT state_code, cresta_zone, region, line_of_business, product, "
     f"total_insured_value_usd, annual_premium_usd, policy_count "
     f"FROM {CATALOG}.{SCHEMA}.exposure_accumulation"),

    ("solvency", "Solvency Capital",
     f"SELECT metric, value_usd FROM {CATALOG}.{SCHEMA}.solvency_capital"),

    ("solvency_kpi", "Solvency KPI",
     f"SELECT "
     f"  MAX(CASE WHEN metric = 'Solvency Ratio' THEN value_usd END) AS solvency_ratio, "
     f"  MAX(CASE WHEN metric = 'SCR (Total)'    THEN value_usd END) AS scr_total, "
     f"  MAX(CASE WHEN metric = 'Own Funds'      THEN value_usd END) AS own_funds, "
     f"  MAX(CASE WHEN metric = 'Market Value'   THEN value_usd END) AS market_value "
     f"FROM {CATALOG}.{SCHEMA}.solvency_capital"),

    ("assets", "Asset Portfolio",
     f"SELECT asset_class, rating, market_value_usd, book_value_usd, "
     f"avg_yield_pct, avg_duration_yrs, avg_esg_score, position_count "
     f"FROM {CATALOG}.{SCHEMA}.asset_portfolio_summary"),

    ("event_risk", "NOAA Event Risk",
     f"SELECT alert_id, event_type, severity, urgency, effective_utc, expires_utc, "
     f"state_code, state_name, exposed_tiv_usd, exposed_policy_count "
     f"FROM {CATALOG}.{SCHEMA}.event_risk_correlation"),

    ("cat_events", "Catastrophe Events",
     f"SELECT event_id, title, event_type, severity, event_time_utc, "
     f"latitude, longitude, state_code, pml_estimate_usd "
     f"FROM {CATALOG}.{SCHEMA}.cat_event_pml"),

    ("event_risk_totals", "Event Risk Totals",
     f"SELECT "
     f"  COUNT(*) AS active_alerts, "
     f"  COUNT(DISTINCT state_code) AS states_with_alerts, "
     f"  SUM(exposed_tiv_usd) AS total_exposed_tiv, "
     f"  SUM(exposed_policy_count) AS total_exposed_policies "
     f"FROM {CATALOG}.{SCHEMA}.event_risk_correlation"),
]


def build_dataset_objects():
    out = []
    for name, display, sql in DATASETS:
        out.append({
            "name": name,
            "displayName": display,
            "queryLines": [sql + " "],
        })
    return out


# ---------------------------------------------------------------------------
# Widget helpers
# ---------------------------------------------------------------------------
def text(name, lines, x, y, w, h):
    return {
        "widget": {
            "name": name,
            "multilineTextboxSpec": {"lines": lines},
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def counter(name, dataset, value_field, value_expr, title, x, y, w=2, h=3,
            disaggregated=True, format_pct=False):
    field_name = value_field if disaggregated else value_field
    spec = {
        "version": 2,
        "widgetType": "counter",
        "encodings": {
            "value": {
                "fieldName": value_field,
                "displayName": title,
            }
        },
        "frame": {"showTitle": True, "title": title},
    }
    if format_pct:
        spec["encodings"]["value"]["format"] = {
            "type": "number-percent",
            "decimalPlaces": {"type": "exact", "places": 1},
        }
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": [{"name": value_field, "expression": value_expr}],
                    "disaggregated": disaggregated,
                }
            }],
            "spec": spec,
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def bar(name, dataset, x_field, x_expr, y_field, y_expr, color_field=None,
        color_expr=None, title="", x=0, y=0, w=3, h=5, colors=None,
        sort=None, layout=None, x_scale="categorical"):
    fields = [
        {"name": x_field, "expression": x_expr},
        {"name": y_field, "expression": y_expr},
    ]
    encodings = {
        "x": {
            "fieldName": x_field,
            "scale": {"type": x_scale},
            "displayName": x_field,
        },
        "y": {
            "fieldName": y_field,
            "scale": {"type": "quantitative"},
            "displayName": y_field,
        },
        "label": {"show": True},
    }
    if sort:
        encodings["x"]["scale"]["sort"] = sort
    if color_field:
        fields.append({"name": color_field, "expression": color_expr})
        encodings["color"] = {
            "fieldName": color_field,
            "scale": {"type": "categorical"},
            "displayName": color_field,
        }
    spec = {
        "version": 3,
        "widgetType": "bar",
        "encodings": encodings,
        "frame": {"showTitle": True, "title": title},
        "mark": {"colors": colors or COLORS},
    }
    if layout:
        spec["mark"]["layout"] = layout
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": fields,
                    "disaggregated": False,
                }
            }],
            "spec": spec,
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def line(name, dataset, x_field, x_expr, y_field, y_expr,
         color_field=None, color_expr=None, title="",
         x=0, y=0, w=6, h=5):
    fields = [
        {"name": x_field, "expression": x_expr},
        {"name": y_field, "expression": y_expr},
    ]
    encodings = {
        "x": {"fieldName": x_field, "scale": {"type": "temporal"}, "displayName": x_field},
        "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_field},
    }
    if color_field:
        fields.append({"name": color_field, "expression": color_expr})
        encodings["color"] = {
            "fieldName": color_field,
            "scale": {"type": "categorical"},
            "displayName": color_field,
        }
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": fields,
                    "disaggregated": False,
                }
            }],
            "spec": {
                "version": 3,
                "widgetType": "line",
                "encodings": encodings,
                "frame": {"showTitle": True, "title": title},
                "mark": {"colors": COLORS},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def pie(name, dataset, angle_field, angle_expr, color_field, color_expr,
        title, x, y, w=3, h=5):
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": [
                        {"name": angle_field, "expression": angle_expr},
                        {"name": color_field, "expression": color_expr},
                    ],
                    "disaggregated": False,
                }
            }],
            "spec": {
                "version": 3,
                "widgetType": "pie",
                "encodings": {
                    "angle": {"fieldName": angle_field, "scale": {"type": "quantitative"},
                              "displayName": angle_field},
                    "color": {"fieldName": color_field, "scale": {"type": "categorical"},
                              "displayName": color_field},
                },
                "frame": {"showTitle": True, "title": title},
                "mark": {"colors": COLORS},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def table(name, dataset, cols, title, x, y, w=6, h=6):
    fields = [{"name": c[0], "expression": f"`{c[2]}`" if len(c) == 3 else f"`{c[0]}`"} for c in cols]
    columns = [{"fieldName": c[0], "displayName": c[1]} for c in cols]
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": fields,
                    "disaggregated": True,
                }
            }],
            "spec": {
                "version": 2,
                "widgetType": "table",
                "encodings": {"columns": columns},
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
def page_executive():
    layout = [
        text(uid("hdr_"), ["# Allianz Insurance Intelligence — Executive Summary"], 0, 0, 6, 1),
        text(uid("sub_"), ["Personal · Commercial · Specialty · gross written premium, loss & combined ratio"],
             0, 1, 6, 1),

        counter(uid("c_"), "kpi_totals", "total_gwp",  "`total_gwp`",
                "Gross Written Premium (USD)", 0, 2),
        counter(uid("c_"), "kpi_totals", "total_nwp",  "`total_nwp`",
                "Net Written Premium (USD)",   2, 2),
        counter(uid("c_"), "kpi_totals", "total_claims", "`total_claims`",
                "Total Claims",                4, 2),

        text(uid("h_"), ["## Premium & Combined Ratio by Line of Business"], 0, 5, 6, 1),

        bar(uid("w_"), "uw_kpis", "line_of_business", "`line_of_business`",
            "sum(gross_written_premium_usd)", "SUM(`gross_written_premium_usd`)",
            title="Gross Written Premium by LOB",
            x=0, y=6, w=3, h=6,
            sort={"by": "y-reversed"}),

        line(uid("w_"), "uw_kpis", "month", "DATE_TRUNC(\"MONTH\", `month`)",
             "avg(combined_ratio)", "AVG(`combined_ratio`)",
             color_field="line_of_business", color_expr="`line_of_business`",
             title="Combined Ratio Trend by LOB",
             x=3, y=6, w=3, h=6),

        text(uid("h_"), ["## Claims Severity by Peril (Top 15)"], 0, 12, 6, 1),

        table(uid("w_"), "claims_summary", [
            ("line_of_business", "LOB", "line_of_business"),
            ("product", "Product", "product"),
            ("peril", "Peril", "peril"),
            ("claim_count", "Claims", "claim_count"),
            ("total_incurred_usd", "Total Incurred (USD)", "total_incurred_usd"),
            ("avg_severity_usd", "Avg Severity (USD)", "avg_severity_usd"),
            ("cat_claim_count", "Cat Claims", "cat_claim_count"),
        ], "Claims by LOB / Product / Peril", 0, 13, 6, 7),
    ]
    return {
        "name": "exec",
        "displayName": "Executive",
        "pageType": "PAGE_TYPE_CANVAS",
        "layout": layout,
    }


def page_exposure():
    layout = [
        text(uid("hdr_"), ["# Exposure & Accumulation"], 0, 0, 6, 1),
        text(uid("sub_"), ["Total insured value (TIV) and policy count by state, CRESTA zone, LOB"],
             0, 1, 6, 1),

        bar(uid("w_"), "exposure", "state_code", "`state_code`",
            "sum(total_insured_value_usd)", "SUM(`total_insured_value_usd`)",
            title="TIV by State (USD)",
            x=0, y=2, w=6, h=6, sort={"by": "y-reversed"}),

        bar(uid("w_"), "exposure", "cresta_zone", "`cresta_zone`",
            "sum(total_insured_value_usd)", "SUM(`total_insured_value_usd`)",
            color_field="region", color_expr="`region`",
            title="TIV by CRESTA Zone (Color by Region)",
            x=0, y=8, w=6, h=6, sort={"by": "y-reversed"}),

        bar(uid("w_"), "exposure", "product", "`product`",
            "sum(annual_premium_usd)", "SUM(`annual_premium_usd`)",
            color_field="line_of_business", color_expr="`line_of_business`",
            title="Annual Premium by Product (Color by LOB)",
            x=0, y=14, w=6, h=6, sort={"by": "y-reversed"}, layout="group"),
    ]
    return {
        "name": "exposure",
        "displayName": "Exposure & Accumulation",
        "pageType": "PAGE_TYPE_CANVAS",
        "layout": layout,
    }


def page_solvency():
    layout = [
        text(uid("hdr_"), ["# Solvency II"], 0, 0, 6, 1),
        text(uid("sub_"), ["Solvency Capital Requirement breakdown, own funds, asset portfolio"],
             0, 1, 6, 1),

        counter(uid("c_"), "solvency_kpi", "solvency_ratio", "`solvency_ratio`",
                "Solvency Ratio", 0, 2),
        counter(uid("c_"), "solvency_kpi", "scr_total", "`scr_total`",
                "SCR (USD)", 2, 2),
        counter(uid("c_"), "solvency_kpi", "own_funds", "`own_funds`",
                "Own Funds (USD)", 4, 2),

        text(uid("h_"), ["## SCR Breakdown & Asset Allocation"], 0, 5, 6, 1),

        bar(uid("w_"), "solvency", "metric", "`metric`",
            "sum(value_usd)", "SUM(`value_usd`)",
            title="Solvency Capital Components", x=0, y=6, w=3, h=6,
            sort={"by": "y-reversed"}),

        pie(uid("w_"), "assets", "sum(market_value_usd)", "SUM(`market_value_usd`)",
            "asset_class", "`asset_class`",
            "Asset Portfolio by Class", 3, 6, 3, 6),

        text(uid("h_"), ["## Asset Quality by Rating"], 0, 12, 6, 1),

        bar(uid("w_"), "assets", "rating", "`rating`",
            "sum(market_value_usd)", "SUM(`market_value_usd`)",
            color_field="asset_class", color_expr="`asset_class`",
            title="Market Value by Credit Rating",
            x=0, y=13, w=6, h=6, sort={"by": "y-reversed"}),
    ]
    return {
        "name": "solvency",
        "displayName": "Solvency II",
        "pageType": "PAGE_TYPE_CANVAS",
        "layout": layout,
    }


def page_realtime():
    layout = [
        text(uid("hdr_"), ["# Real-Time Event Risk Correlation"], 0, 0, 6, 1),
        text(uid("sub_"), ["Active NOAA alerts and catastrophe events overlapped with Allianz exposure"],
             0, 1, 6, 1),

        counter(uid("c_"), "event_risk_totals", "active_alerts", "`active_alerts`",
                "Active NOAA Alerts", 0, 2),
        counter(uid("c_"), "event_risk_totals", "states_with_alerts", "`states_with_alerts`",
                "States Affected", 2, 2),
        counter(uid("c_"), "event_risk_totals", "total_exposed_tiv", "`total_exposed_tiv`",
                "Exposed TIV (USD)", 4, 2),

        text(uid("h_"), ["## Active NOAA Alerts × Exposure"], 0, 5, 6, 1),

        bar(uid("w_"), "event_risk", "event_type", "`event_type`",
            "count(*)", "COUNT(`alert_id`)",
            color_field="severity", color_expr="`severity`",
            title="NOAA Alerts by Type & Severity",
            x=0, y=6, w=3, h=6, sort={"by": "y-reversed"}),

        bar(uid("w_"), "event_risk", "state_code", "`state_code`",
            "sum(exposed_tiv_usd)", "SUM(`exposed_tiv_usd`)",
            title="Exposed TIV by State",
            x=3, y=6, w=3, h=6, sort={"by": "y-reversed"}),

        text(uid("h_"), ["## Catastrophe PML Estimates"], 0, 12, 6, 1),

        table(uid("w_"), "cat_events", [
            ("title", "Event", "title"),
            ("event_type", "Type", "event_type"),
            ("severity", "Severity", "severity"),
            ("state_code", "State", "state_code"),
            ("pml_estimate_usd", "PML Estimate (USD)", "pml_estimate_usd"),
            ("event_time_utc", "Event Time", "event_time_utc"),
        ], "Catastrophe Events with PML Estimate", 0, 13, 6, 8),
    ]
    return {
        "name": "realtime",
        "displayName": "Real-Time Event Risk",
        "pageType": "PAGE_TYPE_CANVAS",
        "layout": layout,
    }


def build_dashboard_json():
    return {
        "datasets": build_dataset_objects(),
        "pages": [
            page_executive(),
            page_exposure(),
            page_solvency(),
            page_realtime(),
        ],
        "uiSettings": {
            "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
            "applyModeEnabled": False,
        },
    }


def main():
    dash = build_dashboard_json()
    serialized = json.dumps(dash, separators=(",", ":"))

    payload = {
        "display_name": "Allianz Insurance Intelligence",
        "warehouse_id": WAREHOUSE,
        "parent_path": "/Workspace/Users/saswata.sengupta@databricks.com",
        "serialized_dashboard": serialized,
    }

    out = Path("dashboards/allianz_dashboard.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(dash, indent=2))
    print(f"Wrote {out}")

    # Look for existing dashboard
    res = subprocess.run(
        ["databricks", "api", "get", "/api/2.0/lakeview/dashboards",
         "--profile", PROFILE],
        capture_output=True, text=True, check=True,
    )
    existing = json.loads(res.stdout or "{}").get("dashboards", [])
    existing_id = next(
        (d["dashboard_id"] for d in existing
         if d.get("display_name") == "Allianz Insurance Intelligence"),
        None,
    )

    payload_path = Path("/tmp/dash_payload.json")
    payload_path.write_text(json.dumps(payload))

    if existing_id:
        print(f"Updating existing dashboard {existing_id}…")
        r = subprocess.run(
            ["databricks", "api", "patch",
             f"/api/2.0/lakeview/dashboards/{existing_id}",
             "--json", f"@{payload_path}",
             "--profile", PROFILE],
            capture_output=True, text=True,
        )
    else:
        print("Creating dashboard…")
        r = subprocess.run(
            ["databricks", "api", "post",
             "/api/2.0/lakeview/dashboards",
             "--json", f"@{payload_path}",
             "--profile", PROFILE],
            capture_output=True, text=True,
        )
    print(r.stdout)
    if r.returncode != 0:
        print("STDERR:", r.stderr)
        raise SystemExit(r.returncode)

    body = json.loads(r.stdout)
    dash_id = body.get("dashboard_id") or existing_id
    print(f"Dashboard ID: {dash_id}")

    # Publish
    pub = subprocess.run(
        ["databricks", "api", "post",
         f"/api/2.0/lakeview/dashboards/{dash_id}/published",
         "--profile", PROFILE,
         "--json", json.dumps({"embed_credentials": True,
                               "warehouse_id": WAREHOUSE})],
        capture_output=True, text=True,
    )
    print("Publish:", pub.stdout[:200])
    print(f"https://fevm-serverless-stable-xhky6g.cloud.databricks.com/dashboardsv3/{dash_id}")


if __name__ == "__main__":
    main()
