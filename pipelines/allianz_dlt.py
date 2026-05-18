"""
Allianz Insurance Intelligence — Lakeflow Declarative Pipeline.

Bronze raw landing is populated by:
  • ``src/generate_data.py``       →  ``allianz_bronze``
  • ``src/external_feeds.py``      →  ``allianz_ext``

The DLT pipeline publishes:
  • Conformed silver dims, facts, external events  →  ``allianz_silver`` (default)
  • Business marts                                  →  ``allianz_gold`` (qualified names)

Cross-table dependencies always use ``dlt.read("<table>")`` so the DAG is
resolved correctly and ordering is enforced.
"""
from pyspark.sql import functions as F
import dlt

CATALOG = spark.conf.get("allianz.catalog")
BRONZE = spark.conf.get("allianz.bronze_schema", "allianz_bronze")
GOLD = spark.conf.get("allianz.gold_schema", "allianz_gold")


# =========================================================================
# BRONZE views — thin pass-through over the raw landing tables.
# (Views live inside the pipeline namespace only, not published to UC.)
# =========================================================================
def _bronze_view(name: str, source_schema: str, source_table: str):
    @dlt.view(name=f"bronze_{name}")
    def _view():
        return spark.table(f"{CATALOG}.{source_schema}.{source_table}")
    return _view


_bronze_view("policies",     BRONZE, "policies_raw")
_bronze_view("customers",    BRONZE, "customers_raw")
_bronze_view("agents",       BRONZE, "agents_raw")
_bronze_view("premiums",     BRONZE, "premiums_raw")
_bronze_view("claims",       BRONZE, "claims_raw")
_bronze_view("geography",    BRONZE, "geography_raw")
_bronze_view("reinsurance",  BRONZE, "reinsurance_treaties_raw")
_bronze_view("assets",       BRONZE, "asset_portfolio_raw")
_bronze_view("weather",      BRONZE, "weather_obs_raw")
_bronze_view("earthquakes",  BRONZE, "earthquakes_raw")
_bronze_view("catastrophes", BRONZE, "catastrophe_events_raw")
_bronze_view("noaa_alerts",  BRONZE, "noaa_alerts_raw")
_bronze_view("news",         BRONZE, "news_raw")

# Bronze views over the migrated allianz_pc P&C reference data.
_bronze_view("pc_policies",             BRONZE, "pc_policies_raw")
_bronze_view("pc_claims",               BRONZE, "pc_claims_raw")
_bronze_view("pc_investment_assets",    BRONZE, "pc_investment_assets_raw")
_bronze_view("pc_reinsurance_treaties", BRONZE, "pc_reinsurance_treaties_raw")
_bronze_view("pc_weather_events",       BRONZE, "pc_weather_events_raw")
_bronze_view("pc_realtime_events",      BRONZE, "pc_realtime_events_raw")


# =========================================================================
# SILVER — conformed dims, facts, external events.
# Unqualified names default to the pipeline target schema (allianz_silver).
# =========================================================================
@dlt.table(name="dim_geography",
           comment="Conformed geography dimension (US states + CRESTA zones).")
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


@dlt.table(name="dim_policy",
           comment="Active and inactive policies across all LOBs.")
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


@dlt.table(name="fact_claim",
           comment="Claims fact with peril and catastrophe tagging.")
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


@dlt.table(name="fact_exposure",
           comment="Per-policy exposure snapshot used for accumulation.")
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


@dlt.table(name="dim_asset",
           comment="Investment portfolio for solvency / asset analytics.")
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


# ----- P&C reference data (silver) — conformed from migrated allianz_pc data.
@dlt.table(name="pc_policies",
           comment="Conformed P&C policy dimension (from migrated allianz_pc).")
def pc_policies():
    return (dlt.read("bronze_pc_policies")
            .withColumn("inception_date", F.to_date("inception_date"))
            .withColumn("expiry_date", F.to_date("expiry_date")))


@dlt.table(name="pc_claims",
           comment="Conformed P&C claims fact (from migrated allianz_pc).")
def pc_claims():
    return (dlt.read("bronze_pc_claims")
            .withColumn("loss_date", F.to_date("loss_date"))
            .withColumn("report_date", F.to_date("report_date"))
            .withColumn("net_paid_usd",
                        F.col("paid_amount") - F.col("reserved_amount") * F.lit(0)))


@dlt.table(name="pc_investment_assets",
           comment="P&C investment portfolio (EUR-denominated) from migrated allianz_pc.")
def pc_investment_assets():
    return dlt.read("bronze_pc_investment_assets").withColumn(
        "maturity_date", F.to_date("maturity_date"))


@dlt.table(name="pc_reinsurance_treaties",
           comment="P&C reinsurance treaties (EUR) from migrated allianz_pc.")
def pc_reinsurance_treaties():
    return (dlt.read("bronze_pc_reinsurance_treaties")
            .withColumn("inception_date", F.to_date("inception_date"))
            .withColumn("expiry_date", F.to_date("expiry_date")))


@dlt.table(name="pc_weather_events",
           comment="Historical weather catastrophe events with insured loss (EUR).")
def pc_weather_events():
    return dlt.read("bronze_pc_weather_events").withColumn(
        "event_date", F.to_date("event_date"))


@dlt.table(name="pc_realtime_events",
           comment="P&C real-time event stream with portfolio relevance scores.")
def pc_realtime_events():
    return dlt.read("bronze_pc_realtime_events").withColumn(
        "event_timestamp", F.to_timestamp("event_timestamp"))


# =========================================================================
# GOLD — business marts. Published to allianz_gold via qualified name=.
# Cross-references to silver use dlt.read() so DAG order is enforced.
# =========================================================================
@dlt.table(
    name=f"{GOLD}.underwriting_kpis",
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
    name=f"{GOLD}.claims_summary",
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
    name=f"{GOLD}.exposure_accumulation",
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
    name=f"{GOLD}.solvency_capital",
    comment="Simplified Solvency II calculation: own funds / SCR / MCR.",
)
def solvency_capital():
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
    name=f"{GOLD}.asset_portfolio_summary",
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
    name=f"{GOLD}.reinsurance_summary",
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
    name=f"{GOLD}.event_risk_correlation",
    comment=("Correlation gold mart: joins active NOAA alerts with per-state exposure "
             "to compute exposed TIV and policy count."),
)
def event_risk_correlation():
    states = (dlt.read("dim_geography")
              .select("state_code", "state_name").distinct())

    alerts = (dlt.read("noaa_alerts")
              .select("alert_id", "event_type", "severity", "urgency",
                      "area_desc", "effective_utc", "expires_utc"))

    alerts_state = (alerts
                    .crossJoin(states)
                    .where(F.col("area_desc").contains(F.col("state_name"))
                           | F.col("area_desc").contains(F.col("state_code"))))

    exposure_state = (dlt.read("fact_exposure").alias("e")
                      .join(dlt.read("dim_geography").alias("g"), "geo_id")
                      .groupBy(F.col("g.state_code").alias("state_code"))
                      .agg(F.sum("total_insured_value_usd").alias("state_tiv_usd"),
                           F.count("*").alias("state_policy_count")))

    return (alerts_state.alias("a")
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


@dlt.table(
    name=f"{GOLD}.cat_event_pml",
    comment="Probable Maximum Loss estimate per catastrophe peril using exposure & severity factors.",
)
def cat_event_pml():
    peril_factor = spark.createDataFrame(
        [("Earthquake", 0.18), ("TropicalCyclone", 0.22), ("Flood", 0.12),
         ("Wildfire", 0.08), ("Drought", 0.03), ("Volcanic", 0.10), ("Other", 0.02)],
        ["event_type", "severity_factor"])

    exposure_state = (dlt.read("fact_exposure").alias("e")
                      .join(dlt.read("dim_geography").alias("g"), "geo_id")
                      .groupBy(F.col("g.state_code").alias("state_code"))
                      .agg(F.sum("total_insured_value_usd").alias("state_tiv_usd")))

    return (dlt.read("catastrophe_events")
            .join(peril_factor, "event_type", "left")
            .crossJoin(exposure_state)
            .withColumn("pml_estimate_usd",
                        F.col("state_tiv_usd") * F.col("severity_factor"))
            .select("event_id", "title", "event_type", "severity",
                    "event_time_utc", "latitude", "longitude",
                    "state_code", "state_tiv_usd", "severity_factor",
                    "pml_estimate_usd"))


# =========================================================================
# GOLD — P&C industry-standard joined views.
# These materialize multi-table joins that analysts repeatedly run, exposing
# them as a single, Genie-friendly surface.
# =========================================================================
@dlt.table(
    name=f"{GOLD}.policy_customer_360",
    comment=("Policy 360: policy + customer + agent + geography. "
             "One row per active policy with full demographic and distribution context."),
)
def policy_customer_360():
    return (dlt.read("dim_policy").alias("p")
            .join(dlt.read("dim_customer").alias("c"), "customer_id", "left")
            .join(dlt.read("dim_geography").alias("g"), "geo_id", "left")
            .join(dlt.read("dim_agent").alias("a"), "agent_id", "left")
            .select(
                F.col("p.policy_id"),
                F.col("p.line_of_business"),
                F.col("p.product"),
                F.col("p.lob_code"),
                F.col("p.policy_status"),
                F.col("p.channel"),
                F.col("p.effective_date"),
                F.col("p.expiration_date"),
                F.col("p.annual_premium_usd"),
                F.col("p.sum_insured_usd"),
                F.col("p.deductible_usd"),
                F.col("p.reinsured"),
                F.col("p.underwriter_score"),
                F.col("c.customer_id"),
                F.col("c.customer_name"),
                F.col("c.customer_type"),
                F.col("c.loyalty_tier"),
                F.col("c.income_bracket"),
                F.col("c.credit_score"),
                F.col("g.state_code"),
                F.col("g.state_name"),
                F.col("g.region"),
                F.col("g.cresta_zone"),
                F.col("g.flood_zone"),
                F.col("g.windstorm_zone"),
                F.col("a.agent_id"),
                F.col("a.agent_name"),
                F.col("a.channel").alias("agent_channel"),
                F.col("a.specialty_lob").alias("agent_specialty_lob"),
            ))


@dlt.table(
    name=f"{GOLD}.claim_360",
    comment=("Claim 360: claim + policy + customer + geography. "
             "One row per claim with peril, severity and the policy it was made against."),
)
def claim_360():
    return (dlt.read("fact_claim").alias("cl")
            .join(dlt.read("dim_policy").alias("p"),
                  F.col("cl.policy_id") == F.col("p.policy_id"), "left")
            .join(dlt.read("dim_customer").alias("c"),
                  F.col("p.customer_id") == F.col("c.customer_id"), "left")
            .join(dlt.read("dim_geography").alias("g"),
                  F.col("cl.geo_id") == F.col("g.geo_id"), "left")
            .select(
                F.col("cl.claim_id"),
                F.col("cl.policy_id"),
                F.col("cl.line_of_business"),
                F.col("cl.product"),
                F.col("cl.peril"),
                F.col("cl.claim_status"),
                F.col("cl.loss_date"),
                F.col("cl.report_date"),
                F.datediff("cl.report_date", "cl.loss_date").alias("report_lag_days"),
                F.col("cl.incurred_amount_usd"),
                F.col("cl.paid_amount_usd"),
                F.col("cl.reserve_amount_usd"),
                F.col("cl.net_paid_usd"),
                F.col("cl.salvage_recovery_usd"),
                F.col("cl.subrogation_recovery_usd"),
                F.col("cl.fraud_flag"),
                F.col("cl.catastrophe_code"),
                F.col("cl.is_cat"),
                F.when(F.col("p.sum_insured_usd") > 0,
                       F.col("cl.incurred_amount_usd") / F.col("p.sum_insured_usd"))
                 .alias("severity_pct_of_sum_insured"),
                F.col("p.annual_premium_usd"),
                F.col("p.sum_insured_usd"),
                F.col("p.channel"),
                F.col("c.customer_name"),
                F.col("c.customer_type"),
                F.col("c.loyalty_tier"),
                F.col("g.state_code"),
                F.col("g.state_name"),
                F.col("g.region"),
                F.col("g.cresta_zone"),
                F.col("g.flood_zone"),
            ))


@dlt.table(
    name=f"{GOLD}.loss_ratio_by_segment",
    comment=("Loss ratio by segment: aggregates premium and incurred loss by LOB / product / "
             "state / channel / customer_type for portfolio analysis."),
)
def loss_ratio_by_segment():
    premiums = (dlt.read("fact_premium").alias("fp")
                .join(dlt.read("dim_policy").alias("p"),
                      F.col("fp.policy_id") == F.col("p.policy_id"))
                .join(dlt.read("dim_customer").alias("c"), "customer_id")
                .join(dlt.read("dim_geography").alias("g"), "geo_id")
                .groupBy(
                    F.col("fp.line_of_business"),
                    F.col("fp.product"),
                    F.col("g.state_code"),
                    F.col("p.channel"),
                    F.col("c.customer_type"),
                )
                .agg(F.sum("gross_premium_usd").alias("gross_premium_usd"),
                     F.sum("net_premium_usd").alias("net_premium_usd"),
                     F.sum("commission_usd").alias("commission_usd")))

    claims = (dlt.read("fact_claim").alias("cl")
              .join(dlt.read("dim_policy").alias("p"),
                    F.col("cl.policy_id") == F.col("p.policy_id"))
              .join(dlt.read("dim_customer").alias("c"), "customer_id")
              .join(dlt.read("dim_geography").alias("g"), "geo_id")
              .groupBy(
                  F.col("cl.line_of_business"),
                  F.col("cl.product"),
                  F.col("g.state_code"),
                  F.col("p.channel"),
                  F.col("c.customer_type"),
              )
              .agg(F.sum("incurred_amount_usd").alias("incurred_loss_usd"),
                   F.count("*").alias("claim_count")))

    return (premiums.join(claims,
                          ["line_of_business", "product", "state_code",
                           "channel", "customer_type"], "left")
            .fillna(0, subset=["incurred_loss_usd", "claim_count"])
            .withColumn("loss_ratio",
                        F.when(F.col("gross_premium_usd") > 0,
                               F.col("incurred_loss_usd") / F.col("gross_premium_usd")).otherwise(0))
            .withColumn("expense_ratio",
                        F.when(F.col("gross_premium_usd") > 0,
                               F.col("commission_usd") / F.col("gross_premium_usd")).otherwise(0))
            .withColumn("combined_ratio",
                        F.col("loss_ratio") + F.col("expense_ratio")))


@dlt.table(
    name=f"{GOLD}.book_health",
    comment=("Book health: portfolio-level P&C KPIs by LOB and product — "
             "policy count, GWP, frequency (claims per 100 policies), severity, loss ratio."),
)
def book_health():
    policy_counts = (dlt.read("dim_policy")
                     .filter(F.col("policy_status") == "Active")
                     .groupBy("line_of_business", "product")
                     .agg(F.count("*").alias("active_policy_count"),
                          F.sum("annual_premium_usd").alias("inforce_premium_usd"),
                          F.sum("sum_insured_usd").alias("inforce_sum_insured_usd"),
                          F.avg("underwriter_score").alias("avg_underwriter_score")))

    claim_metrics = (dlt.read("fact_claim")
                     .groupBy("line_of_business", "product")
                     .agg(F.count("*").alias("claim_count"),
                          F.sum("incurred_amount_usd").alias("incurred_loss_usd"),
                          F.avg("incurred_amount_usd").alias("avg_severity_usd"),
                          F.sum(F.when(F.col("is_cat"), 1).otherwise(0)).alias("cat_claim_count"),
                          F.sum(F.when(F.col("fraud_flag"), 1).otherwise(0)).alias("fraud_claim_count")))

    return (policy_counts.join(claim_metrics, ["line_of_business", "product"], "left")
            .fillna(0)
            .withColumn("claim_frequency_per_100",
                        F.when(F.col("active_policy_count") > 0,
                               F.col("claim_count") * 100.0 / F.col("active_policy_count")).otherwise(0))
            .withColumn("loss_ratio",
                        F.when(F.col("inforce_premium_usd") > 0,
                               F.col("incurred_loss_usd") / F.col("inforce_premium_usd")).otherwise(0))
            .withColumn("avg_premium_per_policy",
                        F.when(F.col("active_policy_count") > 0,
                               F.col("inforce_premium_usd") / F.col("active_policy_count")).otherwise(0)))


@dlt.table(
    name=f"{GOLD}.peril_loss_summary",
    comment="Loss summary by peril across the entire P&C book — frequency, severity, cat share.",
)
def peril_loss_summary():
    return (dlt.read("fact_claim")
            .groupBy("peril")
            .agg(F.count("*").alias("claim_count"),
                 F.sum("incurred_amount_usd").alias("total_incurred_usd"),
                 F.avg("incurred_amount_usd").alias("avg_severity_usd"),
                 F.expr("percentile_approx(incurred_amount_usd, 0.95)")
                  .alias("p95_severity_usd"),
                 F.sum(F.when(F.col("is_cat"), 1).otherwise(0)).alias("cat_claim_count"),
                 F.sum(F.when(F.col("is_cat"), F.col("incurred_amount_usd")).otherwise(0))
                  .alias("cat_incurred_usd"),
                 F.countDistinct("line_of_business").alias("distinct_lobs"))
            .withColumn("cat_share_of_loss",
                        F.when(F.col("total_incurred_usd") > 0,
                               F.col("cat_incurred_usd") / F.col("total_incurred_usd")).otherwise(0)))
