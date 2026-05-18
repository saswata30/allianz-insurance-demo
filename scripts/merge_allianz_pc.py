"""
One-time migration: merge the pre-existing ``allianz_pc`` schema into the
bronze / silver / gold medallion architecture.

Mapping
-------
Raw-style sources → ``allianz_bronze`` (kept as ``pc_*_raw``):

    allianz_pc.policies              → allianz_bronze.pc_policies_raw
    allianz_pc.claims                → allianz_bronze.pc_claims_raw
    allianz_pc.investment_assets     → allianz_bronze.pc_investment_assets_raw
    allianz_pc.reinsurance_treaties  → allianz_bronze.pc_reinsurance_treaties_raw
    allianz_pc.weather_events        → allianz_bronze.pc_weather_events_raw
    allianz_pc.realtime_events       → allianz_bronze.pc_realtime_events_raw

Conformed / aggregated sources → ``allianz_silver``:

    allianz_pc.combined_ratios       → allianz_silver.pc_combined_ratios
    allianz_pc.loss_development      → allianz_silver.pc_loss_development
    allianz_pc.risk_exposure         → allianz_silver.pc_risk_exposure
    allianz_pc.solvency_metrics      → allianz_silver.pc_solvency_metrics

After successful copy the ``allianz_pc`` schema is dropped.

Run:
    uv run --no-project scripts/merge_allianz_pc.py
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

CATALOG = "serverless_stable_xhky6g_catalog"
WAREHOUSE = "cf18de10632b58c8"
PROFILE = "fe-vm-fevm-serverless-stable-xhky6g"

# (source_table, target_schema, target_table)
MIGRATIONS = [
    ("policies",             "allianz_bronze", "pc_policies_raw"),
    ("claims",               "allianz_bronze", "pc_claims_raw"),
    ("investment_assets",    "allianz_bronze", "pc_investment_assets_raw"),
    ("reinsurance_treaties", "allianz_bronze", "pc_reinsurance_treaties_raw"),
    ("weather_events",       "allianz_bronze", "pc_weather_events_raw"),
    ("realtime_events",      "allianz_bronze", "pc_realtime_events_raw"),
    ("combined_ratios",      "allianz_silver", "pc_combined_ratios"),
    ("loss_development",     "allianz_silver", "pc_loss_development"),
    ("risk_exposure",        "allianz_silver", "pc_risk_exposure"),
    ("solvency_metrics",     "allianz_silver", "pc_solvency_metrics"),
]


def run_sql(stmt: str) -> dict:
    payload = json.dumps({
        "warehouse_id": WAREHOUSE,
        "statement": stmt,
        "wait_timeout": "50s",
    })
    res = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--json", payload, "--profile", PROFILE],
        capture_output=True, text=True,
    )
    if res.returncode != 0:
        print("  SQL CLI error:", res.stderr[:300])
        sys.exit(1)
    body = json.loads(res.stdout)
    state = body.get("status", {}).get("state", "?")
    if state == "SUCCEEDED":
        return body
    err = body.get("status", {}).get("error", {}).get("message", "")
    print(f"  ✗ {state}: {err[:300]}")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop-source", action="store_true",
                        help="Drop the allianz_pc schema after a successful merge.")
    args = parser.parse_args()

    print("=" * 70)
    print("MERGE allianz_pc → allianz_bronze / allianz_silver")
    print("=" * 70)

    for src, tgt_schema, tgt_table in MIGRATIONS:
        fq_src = f"{CATALOG}.allianz_pc.{src}"
        fq_tgt = f"{CATALOG}.{tgt_schema}.{tgt_table}"

        # Use CREATE OR REPLACE so the migration is idempotent.
        run_sql(f"CREATE OR REPLACE TABLE {fq_tgt} AS SELECT * FROM {fq_src}")
        body = run_sql(f"SELECT COUNT(*) FROM {fq_tgt}")
        n = body.get("result", {}).get("data_array", [["0"]])[0][0]
        print(f"  ✓ {fq_src:60s} → {fq_tgt}  ({n} rows)")

    if args.drop_source:
        print()
        print("Dropping source schema allianz_pc…")
        run_sql(f"DROP SCHEMA IF EXISTS {CATALOG}.allianz_pc CASCADE")
        print("  ✓ allianz_pc dropped")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
