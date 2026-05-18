"""
Allianz Insurance Intelligence — Lakeflow Declarative Pipeline (DLT).

Bronze → Silver → Gold for three lines of business plus external event/weather
correlation. Pipeline is configured serverless via the DAB.

Catalog / schemas referenced via spark.conf:
    allianz.catalog        — target catalog
    allianz.bronze_schema  — schema where generator landed raw tables
    allianz.ext_schema     — schema where external feeds landed raw tables
    allianz.silver_schema  — silver target schema (== pipeline target)
    allianz.gold_schema    — gold target schema
"""
from pyspark.sql import functions as F
import dlt

CATALOG = spark.conf.get("allianz.catalog")
BRONZE = spark.conf.get("allianz.bronze_schema", "allianz_bronze")
EXT = spark.conf.get("allianz.ext_schema", "allianz_ext")


# =========================================================================
# BRONZE — thin pass-through views over the raw tables produced by
# generate_data.py and external_feeds.py
# =========================================================================
def _bronze_view(name: str, source_schema: str, source_table: str):
    @dlt.view(name=f"bronze_{name}")
    def _view():
        return spark.table(f"{CATALOG}.{source_schema}.{source_table}")
    return _view


_bronze_view("policies",   BRONZE, "policies_raw")
_bronze_view("customers",  BRONZE, "customers_raw")
_bronze_view("agents",     BRONZE, "agents_raw")
_bronze_view("premiums",   BRONZE, "premiums_raw")
_bronze_view("claims",     BRONZE, "claims_raw")
_bronze_view("geography",  BRONZE, "geography_raw")
_bronze_view("reinsurance", BRONZE, "reinsurance_treaties_raw")
_bronze_view("assets",     BRONZE, "asset_portfolio_raw")
_bronze_view("weather",    EXT,    "weather_obs_raw")
_bronze_view("earthquakes", EXT,   "earthquakes_raw")
_bronze_view("catastrophes", EXT,  "catastrophe_events_raw")
_bronze_view("noaa_alerts", EXT,   "noaa_alerts_raw")
_bronze_view("news",       EXT,    "news_raw")


# =========================================================================
# SILVER — conformed dimensions and facts with DQ expectations
# =========================================================================
@dlt.table(
    name="dim_geography",
    comment="Conformed geography dimension (US states + CRESTA zones).",
)
@dlt.expect_or_drop("valid_geo_id", "geo_id IS NOT NULL")
@dlt.expect("valid_lat", "latitude BETWEEN -90 AND 90")
def dim_geography():
    return (dlt.read("bronze_geography")
            .select(
                "geo_id", "state_code", "state_name", "region", "zip_code",
                "cresta_zone", "country",
                F.col("latitude").cast("double").alias("latitude"),
                F.col("longitude").cast("double").alias("longitude"),
                "windstorm_zone", "flood_zone",
            ))


@dlt.table(name="dim_customer", comment="Conformed customer dimension.")
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def dim_customer():
    return (dlt.read("bronze_customers")
            .select(
                "customer_id", "customer_name", "customer_type",
                "email", "phone",
                F.to_date("date_of_birth").alias("date_of_birth"),
                "geo_id", "income_bracket", "credit_score", "loyalty_tier",
                "marketing_consent",
                F.to_date("signup_date").alias("signup_date"),
            ))


@dlt.table(name="dim_agent", comment="Conformed agent / broker dimension.")
def dim_agent():
    return (dlt.read("bronze_agents")
            .select(
                "agent_id", "agent_name", "channel", "license_state",
                "specialty_lob", "ytd_commission_usd",
                F.to_date("hire_date").alias("hire_date"),
                "active",
            ))


@dlt.table(name="dim_policy", comment="Active and inactive policies across all LOBs.")
@dlt.expect_or_drop("valid_policy", "policy_id IS NOT NULL")
@dlt.expect("premium_positive", "annual_premium_usd >= 0")
def dim_policy():
    return (dlt.read("bronze_policies")
            .withColumn("effective_date", F.to_date("effective_date"))
            .withColumn("expiration_date", F.to_date("expiration_date"))
            .withColumn(
                "lob_code",
                F.when(F.col("line_of_business") == "Personal", "PL")
                 .when(F.col("line_of_business") == "Commercial", "CL")
                 .otherwise("SP"),
            ))


@dlt.table(name="fact_premium", comment="Monthly premium installments fact.")
@dlt.expect("premium_amount_valid", "gross_premium_usd >= 0")
def fact_premium():
    return (dlt.read("bronze_premiums")
            .withColumn("billing_month", F.to_date("billing_month"))
            .withColumn("net_premium_usd",
                        F.col("gross_premium_usd") - F.col("ceded_to_reinsurance_usd")))


@dlt.table(name="fact_claim", comment="Claims fact with peril and catastrophe tagging.")
@dlt.expect("amount_positive", "incurred_amount_usd >= 0")
def fact_claim():
    return (dlt.read("bronze_claims")
            .withColumn("loss_date", F.to_date("loss_date"))
            .withColumn("report_date", F.to_date("report_date"))
            .withColumn("net_paid_usd",
                        F.col("paid_amount_usd")
                         - F.col("salvage_recovery_usd")
                         - F.col("subrogation_recovery_usd"))
            .withColumn("is_cat", F.col("catastrophe_code").isNotNull()))


@dlt.table(name="fact_exposure", comment="Per-policy exposure snapshot used for accumulation.")
def fact_exposure():
    return (dlt.read("bronze_policies")
            .select(
                "policy_id", "customer_id", "geo_id", "line_of_business",
                "product",
                F.col("sum_insured_usd").alias("total_insured_value_usd"),
                F.col("annual_premium_usd").alias("annual_premium_usd"),
                F.col("reinsured"),
                F.to_date("effective_date").alias("effective_date"),
                F.to_date("expiration_date").alias("expiration_date"),
            ))


@dlt.table(name="dim_reinsurance_treaty")
def dim_reinsurance_treaty():
    return (dlt.read("bronze_reinsurance")
            .withColumn("treaty_start", F.to_date("treaty_start"))
            .withColumn("treaty_end", F.to_date("treaty_end")))


@dlt.table(name="dim_asset", comment="Investment portfolio for solvency / asset analytics.")
def dim_asset():
    return (dlt.read("bronze_assets")
            .withColumn("purchase_date", F.to_date("purchase_date"))
            .withColumn("maturity_date", F.to_date("maturity_date")))


# ----- External events (silver) -----
@dlt.table(name="weather_observations")
@dlt.expect("valid_lat", "latitude BETWEEN -90 AND 90")
def weather_observations():
    return (dlt.read("bronze_weather")
            .withColumn("observed_time_utc", F.to_timestamp("observed_time_utc"))
            .withColumn("fetched_at_utc", F.to_timestamp("fetched_at_utc")))


@dlt.table(name="catastrophe_events")
def catastrophe_events():
    # GDACS publishes RFC822 timestamps. We try ISO first, then a Spark-3 safe
    # RFC822 pattern, and finally just keep the string if neither parses.
    return (dlt.read("bronze_catastrophes")
            .withColumn(
                "event_time_utc",
                F.coalesce(
                    F.to_timestamp("event_time_utc"),
                    F.to_timestamp("event_time_utc", "EEE, d MMM yyyy HH:mm:ss z"),
                    F.to_timestamp(F.regexp_replace("event_time_utc",
                                                   "^[A-Za-z]{3}, ", ""),
                                   "d MMM yyyy HH:mm:ss z"),
                ),
            ))


@dlt.table(name="earthquake_events")
def earthquake_events():
    return (dlt.read("bronze_earthquakes")
            .withColumn("event_time_utc", F.to_timestamp("event_time_utc")))


@dlt.table(name="noaa_alerts")
def noaa_alerts():
    return (dlt.read("bronze_noaa_alerts")
            .withColumn("effective_utc", F.to_timestamp("effective_utc"))
            .withColumn("expires_utc", F.to_timestamp("expires_utc"))
            .withColumn("sent_utc", F.to_timestamp("sent_utc")))


@dlt.table(name="news_events")
def news_events():
    return dlt.read("bronze_news")


# =========================================================================
# GOLD — business marts
# =========================================================================
@dlt.table(
    name="underwriting_kpis",
    comment="Underwriting KPIs by line of business and month: GWP, NWP, loss ratio, combined ratio.",
)
def underwriting_kpis():
    premiums = (dlt.read("fact_premium")
                .groupBy(F.date_trunc("month", "billing_month").alias("month"),
                         "line_of_business")
                .agg(F.sum("gross_premium_usd").alias("gross_written_premium_usd"),
                     F.sum("net_premium_usd").alias("net_written_premium_usd"),
                     F.sum("commission_usd").alias("commission_usd"),
                     F.sum("ceded_to_reinsurance_usd").alias("ceded_premium_usd")))

    claims = (dlt.read("fact_claim")
              .groupBy(F.date_trunc("month", "loss_date").alias("month"),
                       "line_of_business")
              .agg(F.sum("incurred_amount_usd").alias("incurred_loss_usd"),
                   F.sum("paid_amount_usd").alias("paid_loss_usd"),
                   F.count("*").alias("claim_count")))

    return (premiums.join(claims, ["month", "line_of_business"], "left")
            .fillna(0)
            .withColumn("loss_ratio",
                        F.when(F.col("gross_written_premium_usd") > 0,
                               F.col("incurred_loss_usd") / F.col("gross_written_premium_usd")).otherwise(0))
            .withColumn("expense_ratio",
                        F.when(F.col("gross_written_premium_usd") > 0,
                               F.col("commission_usd") / F.col("gross_written_premium_usd")).otherwise(0))
            .withColumn("combined_ratio",
                        F.col("loss_ratio") + F.col("expense_ratio")))


@dlt.table(
    name="claims_summary",
    comment="Claim frequency and severity by LOB / product / peril.",
)
def claims_summary():
    return (dlt.read("fact_claim")
            .groupBy("line_of_business", "product", "peril")
            .agg(F.count("*").alias("claim_count"),
                 F.sum("incurred_amount_usd").alias("total_incurred_usd"),
                 F.avg("incurred_amount_usd").alias("avg_severity_usd"),
                 F.max("incurred_amount_usd").alias("max_severity_usd"),
                 F.sum(F.when(F.col("is_cat"), 1).otherwise(0)).alias("cat_claim_count"),
                 F.sum(F.when(F.col("fraud_flag"), 1).otherwise(0)).alias("fraud_flag_count")))


@dlt.table(
    name="exposure_accumulation",
    comment="Total insured value (TIV) accumulation by state / CRESTA / LOB / product.",
)
def exposure_accumulation():
    return (dlt.read("fact_exposure").alias("e")
            .join(dlt.read("dim_geography").alias("g"), "geo_id")
            .groupBy(F.col("g.state_code").alias("state_code"),
                     F.col("g.cresta_zone").alias("cresta_zone"),
                     F.col("g.region").alias("region"),
                     "line_of_business", "product")
            .agg(F.sum("total_insured_value_usd").alias("total_insured_value_usd"),
                 F.sum("annual_premium_usd").alias("annual_premium_usd"),
                 F.count("*").alias("policy_count")))


@dlt.table(
    name="solvency_capital",
    comment="Simplified Solvency II calculation: own funds / SCR / MCR.",
)
def solvency_capital():
    # Build aggregates as DataFrames (DLT-friendly — no scalar collect at graph-build time)
    market_value = (dlt.read("dim_asset")
                    .agg(F.sum("market_value_usd").alias("market_value"))
                    .withColumn("_join", F.lit(1)))
    claims_total = (dlt.read("fact_claim")
                    .agg(F.sum("incurred_amount_usd").alias("claims_total"))
                    .withColumn("_join", F.lit(1)))
    premium_total = (dlt.read("fact_premium")
                     .agg(F.sum("gross_premium_usd").alias("premium_total"))
                     .withColumn("_join", F.lit(1)))

    base = (market_value
            .join(claims_total, "_join")
            .join(premium_total, "_join")
            .withColumn("underwriting_risk",
                        F.col("premium_total") * F.lit(0.18) + F.col("claims_total") * F.lit(0.10))
            .withColumn("market_risk",        F.col("market_value") * F.lit(0.07))
            .withColumn("counterparty_risk",  F.col("market_value") * F.lit(0.015))
            .withColumn("operational_risk",   F.col("premium_total") * F.lit(0.03))
            .withColumn("scr",
                        F.col("underwriting_risk") + F.col("market_risk")
                        + F.col("counterparty_risk") + F.col("operational_risk"))
            .withColumn("mcr",                F.col("scr") * F.lit(0.45))
            .withColumn("own_funds",          F.col("market_value") * F.lit(0.35))
            .withColumn("solvency_ratio",
                        F.when(F.col("scr") > 0, F.col("own_funds") / F.col("scr")).otherwise(0.0)))

    return base.selectExpr(
        "stack(9, "
        "  'Underwriting Risk', underwriting_risk, "
        "  'Market Risk',       market_risk, "
        "  'Counterparty Risk', counterparty_risk, "
        "  'Operational Risk',  operational_risk, "
        "  'SCR (Total)',       scr, "
        "  'MCR (Minimum)',     mcr, "
        "  'Own Funds',         own_funds, "
        "  'Solvency Ratio',    solvency_ratio, "
        "  'Market Value',      market_value"
        ") AS (metric, value_usd)"
    )


@dlt.table(
    name="asset_portfolio_summary",
    comment="Investment portfolio breakdown by class & rating for asset risk view.",
)
def asset_portfolio_summary():
    return (dlt.read("dim_asset")
            .groupBy("asset_class", "rating")
            .agg(F.sum("market_value_usd").alias("market_value_usd"),
                 F.sum("book_value_usd").alias("book_value_usd"),
                 F.avg("yield_pct").alias("avg_yield_pct"),
                 F.avg("duration_yrs").alias("avg_duration_yrs"),
                 F.avg("esg_score").alias("avg_esg_score"),
                 F.count("*").alias("position_count")))


@dlt.table(
    name="reinsurance_summary",
    comment="Reinsurance treaty summary by LOB and reinsurer.",
)
def reinsurance_summary():
    return (dlt.read("dim_reinsurance_treaty")
            .groupBy("line_of_business", "reinsurer", "treaty_type")
            .agg(F.sum("limit_usd").alias("total_limit_usd"),
                 F.sum("premium_ceded_usd").alias("premium_ceded_usd"),
                 F.avg("cession_pct").alias("avg_cession_pct"),
                 F.count("*").alias("treaty_count")))


@dlt.table(
    name="event_risk_correlation",
    comment=("Correlation gold mart: joins active external events / NOAA alerts / weather "
             "with policy exposure by state, to compute exposed TIV and policy count."),
)
def event_risk_correlation():
    # 1) Map NOAA alerts to states by parsing area_desc for state abbreviations
    states = (dlt.read("dim_geography")
              .select("state_code", "state_name").distinct())

    alerts = (dlt.read("noaa_alerts")
              .select("alert_id", "event_type", "severity", "urgency",
                      "area_desc", "effective_utc", "expires_utc"))

    alerts_state = (alerts
                    .crossJoin(states)
                    .where(F.col("area_desc").contains(F.col("state_name"))
                           | F.col("area_desc").contains(F.col("state_code"))))

    # 2) Build exposure by state from fact_exposure
    exposure_state = (dlt.read("fact_exposure").alias("e")
                      .join(dlt.read("dim_geography").alias("g"), "geo_id")
                      .groupBy(F.col("g.state_code").alias("state_code"))
                      .agg(F.sum("total_insured_value_usd").alias("state_tiv_usd"),
                           F.count("*").alias("state_policy_count")))

    # 3) Join alerts to exposure
    correlation = (alerts_state.alias("a")
                   .join(exposure_state.alias("x"),
                         F.col("a.state_code") == F.col("x.state_code"), "left")
                   .select(
                       F.col("a.alert_id"),
                       F.col("a.event_type"),
                       F.col("a.severity"),
                       F.col("a.urgency"),
                       F.col("a.effective_utc"),
                       F.col("a.expires_utc"),
                       F.col("a.state_code"),
                       F.col("a.state_name"),
                       F.col("x.state_tiv_usd").alias("exposed_tiv_usd"),
                       F.col("x.state_policy_count").alias("exposed_policy_count"),
                   ))
    return correlation


@dlt.table(
    name="cat_event_pml",
    comment="Probable Maximum Loss estimate per catastrophe peril using exposure & severity factors.",
)
def cat_event_pml():
    """Toy PML view: exposure * peril-severity factor for the worst recent events."""
    peril_factor = spark.createDataFrame(
        [("Earthquake", 0.18), ("TropicalCyclone", 0.22), ("Flood", 0.12),
         ("Wildfire", 0.08), ("Drought", 0.03), ("Volcanic", 0.10), ("Other", 0.02)],
        ["event_type", "severity_factor"])

    exposure_state = (dlt.read("fact_exposure").alias("e")
                      .join(dlt.read("dim_geography").alias("g"), "geo_id")
                      .groupBy(F.col("g.state_code").alias("state_code"))
                      .agg(F.sum("total_insured_value_usd").alias("state_tiv_usd")))

    # Combine catastrophe events with peril factor and country-level exposure
    return (dlt.read("catastrophe_events")
            .join(peril_factor, "event_type", "left")
            .crossJoin(exposure_state)
            .withColumn("pml_estimate_usd",
                        F.col("state_tiv_usd") * F.col("severity_factor"))
            .select("event_id", "title", "event_type", "severity",
                    "event_time_utc", "latitude", "longitude",
                    "state_code", "state_tiv_usd", "severity_factor",
                    "pml_estimate_usd"))
