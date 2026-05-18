"""
Run every .sql file in ./sql/ against the workspace warehouse to verify it
parses and returns rows. Strips trailing semicolons (the statement API rejects them).
"""
from __future__ import annotations

import json
import pathlib
import subprocess
import sys

WAREHOUSE = "cf18de10632b58c8"
PROFILE = "fe-vm-fevm-serverless-stable-xhky6g"


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
    return json.loads(res.stdout)


def main():
    sql_dir = pathlib.Path("sql")
    rc = 0
    for path in sorted(sql_dir.glob("*.sql")):
        stmt = path.read_text().strip().rstrip(";")
        body = run_sql(stmt)
        state = body.get("status", {}).get("state", "?")
        err = body.get("status", {}).get("error", {}).get("message", "")
        rows = body.get("manifest", {}).get("total_row_count", 0)
        cols = body.get("manifest", {}).get("schema", {}).get("column_count", 0)
        flag = "✓" if state == "SUCCEEDED" else "✗"
        print(f"  {flag} {path.name:42s}  {state:9s}  cols={cols}  rows={rows}")
        if state != "SUCCEEDED":
            print(f"    └─ {err[:200]}")
            rc = 1
    sys.exit(rc)


if __name__ == "__main__":
    main()
