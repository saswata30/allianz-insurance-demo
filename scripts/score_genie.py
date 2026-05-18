"""
Pull the live Genie space from Databricks and score it with the workbench
IQ scanner (vendored at scripts/genie_workbench/scoring.py).
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

PROFILE = "fe-vm-fevm-serverless-stable-xhky6g"
TITLE = "Allianz Insurance Intelligence — Genie"


def run(args):
    res = subprocess.run(args, capture_output=True, text=True)
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


def fetch_serialized(space_id: str) -> dict:
    # The CLI's `genie get-space` drops serialized_space. Use the REST API
    # with ?include_serialized_space=true (same call the workbench scanner uses).
    out = run(["databricks", "api", "get",
               f"/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
               "--profile", PROFILE])
    body = json.loads(out)
    ss = body.get("serialized_space")
    if not ss:
        return {"version": 2, "data_sources": {}, "instructions": {}}
    return json.loads(ss) if isinstance(ss, str) else ss


def main():
    sys.path.insert(0, str(Path(__file__).parent))
    from genie_workbench import calculate_score

    space_id = find_space()
    print(f"Genie space: {space_id}")
    space = fetch_serialized(space_id)
    Path("genie/live_space.json").write_text(json.dumps(space, indent=2))
    print(f"  → wrote genie/live_space.json")
    print()

    report = calculate_score(space)
    print(f"Score: {report['score']}/{report['total']}  ({report['maturity']})")
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
        print("Warnings:")
        for w in report["warnings"]:
            print(f"   ⚠ {w}")
    if report.get("findings"):
        print()
        print("Findings:")
        for f in report["findings"]:
            print(f"   ✗ {f}")
    if report.get("next_steps"):
        print()
        print("Next steps:")
        for n in report["next_steps"]:
            print(f"   • {n}")


if __name__ == "__main__":
    main()
