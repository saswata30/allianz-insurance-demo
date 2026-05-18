"""
Allianz synthetic data generator.

Generates Personal Lines (auto/home/life), Commercial Lines (property/liability/WC),
and Specialty (marine/aviation/cyber) policies, claims, premiums, customers, agents,
reinsurance treaties, geography, and an investment asset portfolio.

Runs locally with Polars + NumPy + Mimesis and writes Delta tables to UC bronze schema
via the Databricks Connect serverless bridge.

Usage:
    uv run --with polars --with numpy --with mimesis \
        --with "databricks-connect>=16.4,<17.0" \
        src/generate_data.py
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime

import numpy as np
import polars as pl
from mimesis import Address, Finance, Generic, Person
from mimesis.locales import Locale

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
DEFAULT_CATALOG = "serverless_stable_xhky6g_catalog"
DEFAULT_SCHEMA = "allianz_bronze"
SEED = 42

# Volumes
N_CUSTOMERS = 50_000
N_AGENTS = 600
N_POLICIES = 120_000   # split across LOBs
N_CLAIMS = 18_000
N_TREATIES = 40
N_ASSETS = 800
N_PREMIUM_MONTHS = 24

RNG = np.random.default_rng(SEED)
GEN = Generic(locale=Locale.EN, seed=SEED)
PERSON = Person(locale=Locale.EN, seed=SEED)
ADDR = Address(locale=Locale.EN, seed=SEED)
FIN = Finance(seed=SEED)

US_STATES = [
    ("CA", "California", "West", 37.7, -122.4),
    ("TX", "Texas", "South", 30.3, -97.7),
    ("NY", "New York", "Northeast", 40.7, -74.0),
    ("FL", "Florida", "South", 27.9, -82.5),
    ("IL", "Illinois", "Midwest", 41.9, -87.6),
    ("PA", "Pennsylvania", "Northeast", 40.0, -75.2),
    ("OH", "Ohio", "Midwest", 39.9, -83.0),
    ("GA", "Georgia", "South", 33.8, -84.4),
    ("NC", "North Carolina", "South", 35.7, -78.6),
    ("MI", "Michigan", "Midwest", 42.3, -83.0),
    ("NJ", "New Jersey", "Northeast", 40.7, -74.2),
    ("VA", "Virginia", "South", 37.5, -77.4),
    ("WA", "Washington", "West", 47.6, -122.3),
    ("AZ", "Arizona", "West", 33.4, -112.0),
    ("MA", "Massachusetts", "Northeast", 42.3, -71.0),
    ("LA", "Louisiana", "South", 29.9, -90.0),
    ("CO", "Colorado", "West", 39.7, -104.9),
]

LOB = ["Personal", "Commercial", "Specialty"]
LOB_WEIGHTS = np.array([0.55, 0.30, 0.15])

PRODUCT_BY_LOB = {
    "Personal":   ["Auto", "Home", "Life", "Renters", "Umbrella"],
    "Commercial": ["Property", "GeneralLiability", "WorkersComp", "DandO", "ProfessionalLiability"],
    "Specialty":  ["Marine", "Aviation", "Cyber", "EnvironmentalLiability", "Energy"],
}

PERILS_BY_PRODUCT = {
    "Auto":       ["Collision", "Theft", "Weather"],
    "Home":       ["Fire", "Wind", "Flood", "Theft"],
    "Life":       ["Mortality"],
    "Renters":    ["Theft", "Fire"],
    "Umbrella":   ["Liability"],
    "Property":   ["Fire", "Flood", "Wind", "Earthquake"],
    "GeneralLiability":     ["BodilyInjury", "PropertyDamage"],
    "WorkersComp":          ["Injury", "Illness"],
    "DandO":                ["RegulatoryAction", "ShareholderSuit"],
    "ProfessionalLiability": ["Errors", "Omissions"],
    "Marine":     ["HullDamage", "CargoLoss", "Piracy"],
    "Aviation":   ["HullDamage", "ThirdPartyLiability"],
    "Cyber":      ["Ransomware", "DataBreach", "BusinessInterruption"],
    "EnvironmentalLiability": ["Pollution", "Remediation"],
    "Energy":     ["WellControl", "BlowOut"],
}

PREMIUM_RANGE_BY_PRODUCT = {  # (low, high) annual premium USD
    "Auto":   (600, 3500),
    "Home":   (900, 6000),
    "Life":   (300, 5000),
    "Renters": (150, 600),
    "Umbrella": (300, 1200),
    "Property": (5000, 250_000),
    "GeneralLiability": (3000, 80_000),
    "WorkersComp": (4000, 150_000),
    "DandO": (10_000, 300_000),
    "ProfessionalLiability": (4000, 60_000),
    "Marine": (20_000, 500_000),
    "Aviation": (50_000, 2_000_000),
    "Cyber": (5000, 250_000),
    "EnvironmentalLiability": (20_000, 600_000),
    "Energy": (40_000, 1_500_000),
}

CLAIM_STATUS = ["Open", "InvestigationInProgress", "Approved", "Denied", "Closed", "Litigation"]
POLICY_STATUS = ["Active", "Lapsed", "Cancelled", "Pending"]
CHANNEL = ["Agent", "Broker", "Direct", "Digital", "Aggregator"]


def _pool(fn, n=1500):
    return np.array([fn() for _ in range(n)])


# -----------------------------------------------------------------------------
# Generators
# -----------------------------------------------------------------------------
def gen_geography() -> pl.DataFrame:
    rows = []
    for code, name, region, lat, lon in US_STATES:
        for i in range(20):  # 20 ZIPs per state
            rows.append({
                "geo_id": f"{code}-{i:02d}",
                "state_code": code,
                "state_name": name,
                "region": region,
                "zip_code": f"{abs(hash((code, i))) % 90000 + 10000:05d}",
                "cresta_zone": f"USA-{code}-{(i % 4) + 1}",
                "latitude": float(lat + (i % 5) * 0.1 - 0.2),
                "longitude": float(lon + (i % 5) * 0.1 - 0.2),
                "country": "USA",
                "windstorm_zone": (i % 3) + 1,
                "flood_zone": ["X", "A", "AE", "V"][i % 4],
            })
    return pl.DataFrame(rows)


def gen_customers(geo: pl.DataFrame) -> pl.DataFrame:
    n = N_CUSTOMERS
    first = _pool(PERSON.first_name, 800)
    last = _pool(PERSON.last_name, 800)
    companies = _pool(FIN.company, 500)

    is_business = RNG.choice([0, 1], size=n, p=[0.75, 0.25])
    geo_ids = geo["geo_id"].to_numpy()

    customer_type = np.where(is_business == 1, "Business", "Individual")
    customer_name = np.where(
        is_business == 1,
        companies[RNG.integers(0, len(companies), size=n)],
        np.char.add(np.char.add(first[RNG.integers(0, len(first), size=n)], " "),
                    last[RNG.integers(0, len(last), size=n)])
    )
    start = np.datetime64("2015-01-01")
    span = (np.datetime64("2025-01-01") - start).astype(int)
    signup = start + RNG.integers(0, span, size=n).astype("timedelta64[D]")

    return pl.DataFrame({
        "customer_id": [f"C{1_000_000 + i}" for i in range(n)],
        "customer_name": customer_name,
        "customer_type": customer_type,
        "email": [f"c{1_000_000 + i}@example.com" for i in range(n)],
        "phone": [PERSON.phone_number() for _ in range(min(n, 5000))]
                 + [None] * (n - min(n, 5000)),
        "date_of_birth": (start + RNG.integers(-25 * 365, 0, size=n).astype("timedelta64[D]")).astype(str),
        "geo_id": geo_ids[RNG.integers(0, len(geo_ids), size=n)],
        "income_bracket": RNG.choice(
            ["<50k", "50-100k", "100-200k", "200k-500k", ">500k"],
            size=n, p=[0.35, 0.32, 0.20, 0.10, 0.03],
        ),
        "credit_score": RNG.integers(550, 850, size=n),
        "loyalty_tier": RNG.choice(
            ["Bronze", "Silver", "Gold", "Platinum"], size=n, p=[0.45, 0.30, 0.18, 0.07],
        ),
        "marketing_consent": RNG.choice([True, False], size=n, p=[0.65, 0.35]),
        "signup_date": signup.astype(str),
    })


def gen_agents(geo: pl.DataFrame) -> pl.DataFrame:
    n = N_AGENTS
    first = _pool(PERSON.first_name, 200)
    last = _pool(PERSON.last_name, 200)
    geo_ids = geo["geo_id"].to_numpy()

    return pl.DataFrame({
        "agent_id": [f"A{20_000 + i}" for i in range(n)],
        "agent_name": np.char.add(np.char.add(first[RNG.integers(0, len(first), size=n)], " "),
                                  last[RNG.integers(0, len(last), size=n)]),
        "channel": RNG.choice(CHANNEL, size=n, p=[0.45, 0.20, 0.15, 0.10, 0.10]),
        "license_state": [g.split("-")[0] for g in geo_ids[RNG.integers(0, len(geo_ids), size=n)]],
        "specialty_lob": RNG.choice(LOB, size=n, p=LOB_WEIGHTS),
        "ytd_commission_usd": np.round(RNG.gamma(2.0, 30_000, size=n), 2),
        "hire_date": (np.datetime64("2010-01-01") +
                      RNG.integers(0, 15 * 365, size=n).astype("timedelta64[D]")).astype(str),
        "active": RNG.choice([True, False], size=n, p=[0.92, 0.08]),
    })


def gen_policies(customers: pl.DataFrame, agents: pl.DataFrame) -> pl.DataFrame:
    n = N_POLICIES
    cust_ids = customers["customer_id"].to_numpy()
    geo_ids = customers["geo_id"].to_numpy()
    agent_ids = agents["agent_id"].to_numpy()

    lob = RNG.choice(LOB, size=n, p=LOB_WEIGHTS)
    product = np.array([RNG.choice(PRODUCT_BY_LOB[l]) for l in lob])

    eff_start = np.datetime64("2022-01-01")
    eff_span = (np.datetime64("2025-12-31") - eff_start).astype(int)
    eff = eff_start + RNG.integers(0, eff_span, size=n).astype("timedelta64[D]")
    exp = eff + np.timedelta64(365, "D")

    annual_premium = np.empty(n, dtype=np.float64)
    for i, p in enumerate(product):
        lo, hi = PREMIUM_RANGE_BY_PRODUCT[p]
        annual_premium[i] = float(RNG.uniform(lo, hi))

    sum_insured = annual_premium * RNG.uniform(40, 500, size=n)
    deductible = annual_premium * RNG.uniform(0.05, 0.30, size=n)

    cust_idx = RNG.integers(0, len(cust_ids), size=n)

    return pl.DataFrame({
        "policy_id": [f"P{5_000_000 + i}" for i in range(n)],
        "customer_id": cust_ids[cust_idx],
        "geo_id": geo_ids[cust_idx],
        "agent_id": agent_ids[RNG.integers(0, len(agent_ids), size=n)],
        "line_of_business": lob,
        "product": product,
        "policy_status": RNG.choice(POLICY_STATUS, size=n, p=[0.80, 0.08, 0.05, 0.07]),
        "channel": RNG.choice(CHANNEL, size=n, p=[0.45, 0.20, 0.15, 0.12, 0.08]),
        "effective_date": eff.astype(str),
        "expiration_date": exp.astype(str),
        "annual_premium_usd": np.round(annual_premium, 2),
        "sum_insured_usd": np.round(sum_insured, 2),
        "deductible_usd": np.round(deductible, 2),
        "policy_term_months": np.array([12] * n),
        "currency": np.array(["USD"] * n),
        "reinsured": RNG.choice([True, False], size=n, p=[0.35, 0.65]),
        "underwriter_score": np.round(RNG.beta(5, 2, size=n) * 100, 1),
    })


def gen_premiums(policies: pl.DataFrame) -> pl.DataFrame:
    """Monthly premium installments for the last N_PREMIUM_MONTHS."""
    pol_ids = policies["policy_id"].to_numpy()
    annual = policies["annual_premium_usd"].to_numpy()
    lobs = policies["line_of_business"].to_numpy()
    products = policies["product"].to_numpy()

    months = []
    base = np.datetime64("2024-01-01")
    for i in range(N_PREMIUM_MONTHS):
        months.append(base + np.timedelta64(i * 30, "D"))
    months = np.array(months)

    sample = RNG.integers(0, len(pol_ids), size=N_PREMIUM_MONTHS * 4000)
    pol = pol_ids[sample]
    lob = lobs[sample]
    prod = products[sample]
    mo = months[RNG.integers(0, len(months), size=len(sample))]
    amt = (annual[sample] / 12.0) * RNG.normal(1.0, 0.05, size=len(sample))
    paid = RNG.choice([True, False], size=len(sample), p=[0.93, 0.07])
    commission = amt * RNG.uniform(0.05, 0.18, size=len(sample))

    return pl.DataFrame({
        "premium_id": [f"PR{10_000_000 + i}" for i in range(len(sample))],
        "policy_id": pol,
        "line_of_business": lob,
        "product": prod,
        "billing_month": mo.astype(str),
        "gross_premium_usd": np.round(amt, 2),
        "commission_usd": np.round(commission, 2),
        "tax_usd": np.round(amt * 0.06, 2),
        "paid": paid,
        "ceded_to_reinsurance_usd": np.round(amt * RNG.uniform(0, 0.40, size=len(sample)), 2),
    })


def gen_claims(policies: pl.DataFrame) -> pl.DataFrame:
    n = N_CLAIMS
    pol_ids = policies["policy_id"].to_numpy()
    products = policies["product"].to_numpy()
    lobs = policies["line_of_business"].to_numpy()
    sums = policies["sum_insured_usd"].to_numpy()
    geos = policies["geo_id"].to_numpy()

    idx = RNG.integers(0, len(pol_ids), size=n)
    pol = pol_ids[idx]
    prod = products[idx]
    lob = lobs[idx]
    si = sums[idx]
    geo = geos[idx]
    peril = np.array([RNG.choice(PERILS_BY_PRODUCT[p]) for p in prod])

    loss_date_start = np.datetime64("2023-06-01")
    loss_span = (np.datetime64("2025-12-15") - loss_date_start).astype(int)
    loss_date = loss_date_start + RNG.integers(0, loss_span, size=n).astype("timedelta64[D]")
    report_lag = RNG.integers(0, 45, size=n).astype("timedelta64[D]")
    report_date = loss_date + report_lag

    # Severity: log-normal of sum insured, capped
    severity_pct = np.clip(RNG.beta(1.2, 8, size=n), 0.005, 0.95)
    incurred = np.round(si * severity_pct, 2)
    paid = incurred * RNG.uniform(0.6, 1.0, size=n)
    reserved = incurred - paid

    return pl.DataFrame({
        "claim_id": [f"CL{30_000_000 + i}" for i in range(n)],
        "policy_id": pol,
        "line_of_business": lob,
        "product": prod,
        "peril": peril,
        "geo_id": geo,
        "loss_date": loss_date.astype(str),
        "report_date": report_date.astype(str),
        "claim_status": RNG.choice(CLAIM_STATUS, size=n, p=[0.20, 0.10, 0.30, 0.07, 0.30, 0.03]),
        "incurred_amount_usd": np.round(incurred, 2),
        "paid_amount_usd": np.round(paid, 2),
        "reserve_amount_usd": np.round(reserved, 2),
        "salvage_recovery_usd": np.round(paid * RNG.uniform(0, 0.10, size=n), 2),
        "subrogation_recovery_usd": np.round(paid * RNG.uniform(0, 0.05, size=n), 2),
        "fraud_flag": RNG.choice([True, False], size=n, p=[0.03, 0.97]),
        "catastrophe_code": np.where(RNG.uniform(size=n) < 0.10,
                                     RNG.choice(["CAT-HU-2024", "CAT-EQ-2024", "CAT-WF-2025", "CAT-FL-2025"], size=n),
                                     None),
    })


def gen_reinsurance(policies: pl.DataFrame) -> pl.DataFrame:
    n = N_TREATIES
    types = ["QuotaShare", "SurplusShare", "ExcessOfLoss", "Stop-Loss", "CatastropheXL"]
    reinsurers = ["Munich Re", "Swiss Re", "Hannover Re", "SCOR", "Lloyd's", "Berkshire Hathaway Re"]

    return pl.DataFrame({
        "treaty_id": [f"T{900 + i}" for i in range(n)],
        "treaty_type": RNG.choice(types, size=n),
        "reinsurer": RNG.choice(reinsurers, size=n),
        "line_of_business": RNG.choice(LOB, size=n, p=LOB_WEIGHTS),
        "cession_pct": np.round(RNG.uniform(0.10, 0.80, size=n), 3),
        "limit_usd": np.round(RNG.gamma(3, 5_000_000, size=n), 2),
        "retention_usd": np.round(RNG.gamma(2, 800_000, size=n), 2),
        "premium_ceded_usd": np.round(RNG.gamma(3, 2_500_000, size=n), 2),
        "treaty_start": (np.datetime64("2023-01-01") +
                        RNG.integers(0, 730, size=n).astype("timedelta64[D]")).astype(str),
        "treaty_end": (np.datetime64("2025-12-31") +
                      RNG.integers(0, 365, size=n).astype("timedelta64[D]")).astype(str),
        "active": RNG.choice([True, False], size=n, p=[0.88, 0.12]),
    })


def gen_assets() -> pl.DataFrame:
    n = N_ASSETS
    asset_classes = ["Govt Bond", "Corp Bond IG", "Corp Bond HY", "Equity", "Real Estate", "MMF", "Mortgage"]
    weights = [0.42, 0.25, 0.05, 0.13, 0.08, 0.04, 0.03]
    rating = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "NotRated"]
    rating_w = [0.20, 0.30, 0.25, 0.15, 0.05, 0.03, 0.01, 0.01]

    cls = RNG.choice(asset_classes, size=n, p=weights)
    rt = RNG.choice(rating, size=n, p=rating_w)
    mv = np.round(RNG.gamma(2.5, 1_500_000, size=n), 2)
    bv = mv * RNG.uniform(0.92, 1.08, size=n)

    duration = np.where(np.isin(cls, ["Equity", "Real Estate", "MMF"]),
                        RNG.uniform(0, 2, size=n),
                        RNG.uniform(1, 15, size=n))
    yld = np.where(rt == "AAA", RNG.uniform(0.02, 0.04, size=n),
          np.where(rt == "BBB", RNG.uniform(0.04, 0.07, size=n),
                                RNG.uniform(0.03, 0.10, size=n)))

    return pl.DataFrame({
        "asset_id": [f"AS{600_000 + i}" for i in range(n)],
        "asset_class": cls,
        "rating": rt,
        "issuer": np.array([FIN.company() for _ in range(n)]),
        "currency": RNG.choice(["USD", "EUR", "GBP", "JPY"], size=n, p=[0.70, 0.18, 0.07, 0.05]),
        "market_value_usd": mv,
        "book_value_usd": np.round(bv, 2),
        "duration_yrs": np.round(duration, 2),
        "yield_pct": np.round(yld, 4),
        "purchase_date": (np.datetime64("2018-01-01") +
                         RNG.integers(0, 7 * 365, size=n).astype("timedelta64[D]")).astype(str),
        "maturity_date": (np.datetime64("2026-01-01") +
                         RNG.integers(0, 25 * 365, size=n).astype("timedelta64[D]")).astype(str),
        "esg_score": np.round(RNG.uniform(20, 95, size=n), 1),
    })


# -----------------------------------------------------------------------------
# Driver
# -----------------------------------------------------------------------------
def write_uc(spark, df: pl.DataFrame, catalog: str, schema: str, table: str):
    print(f"  → {catalog}.{schema}.{table}  ({df.height:,} rows)")
    spark_df = spark.createDataFrame(df.to_pandas())
    (spark_df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{catalog}.{schema}.{table}"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.environ.get("ALLIANZ_CATALOG", DEFAULT_CATALOG))
    parser.add_argument("--schema", default=os.environ.get("ALLIANZ_SCHEMA", DEFAULT_SCHEMA))
    parser.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE",
                                                            "fe-vm-fevm-serverless-stable-xhky6g"))
    parser.add_argument("--local-only", action="store_true",
                        help="Write parquet locally instead of UC")
    args = parser.parse_args()

    started = datetime.now()
    print("=" * 70)
    print("ALLIANZ SYNTHETIC DATA GENERATOR")
    print("=" * 70)
    print(f"Catalog : {args.catalog}")
    print(f"Schema  : {args.schema}")
    print(f"Profile : {args.profile}")
    print(f"Started : {started.isoformat()}")
    print()

    print("Generating geography…");      geo = gen_geography()
    print("Generating customers…");      customers = gen_customers(geo)
    print("Generating agents…");         agents = gen_agents(geo)
    print("Generating policies…");       policies = gen_policies(customers, agents)
    print("Generating premiums…");       premiums = gen_premiums(policies)
    print("Generating claims…");         claims = gen_claims(policies)
    print("Generating reinsurance…");    treaties = gen_reinsurance(policies)
    print("Generating assets…");         assets = gen_assets()

    tables = {
        "geography_raw": geo,
        "customers_raw": customers,
        "agents_raw": agents,
        "policies_raw": policies,
        "premiums_raw": premiums,
        "claims_raw": claims,
        "reinsurance_treaties_raw": treaties,
        "asset_portfolio_raw": assets,
    }

    if args.local_only:
        os.makedirs("output", exist_ok=True)
        for name, df in tables.items():
            path = f"output/{name}.parquet"
            df.write_parquet(path)
            print(f"  → {path}  ({df.height:,} rows)")
    else:
        from databricks.connect import DatabricksSession
        os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile
        spark = (DatabricksSession.builder
                 .profile(args.profile)
                 .serverless()
                 .getOrCreate())
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
        for name, df in tables.items():
            write_uc(spark, df, args.catalog, args.schema, name)
        spark.stop()

    print()
    print(f"Completed in {(datetime.now() - started).total_seconds():.1f}s")


if __name__ == "__main__":
    main()
