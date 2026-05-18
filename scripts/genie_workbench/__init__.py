"""Vendored IQ scoring module from databricks-solutions/databricks-genie-workbench.

Source: packages/genie-space-optimizer/src/genie_space_optimizer/iq_scan/scoring.py
This is a deterministic, rule-based scoring engine that returns 0-12 points and
a maturity tier (Not Ready / Ready to Optimize / Trusted).
"""
from .scoring import calculate_score, get_maturity_label  # noqa: F401
