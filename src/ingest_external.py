"""
Volume → bronze ingestion for the external feeds.

Reads every parquet file from
    /Volumes/<catalog>/allianz_bronze/landing/external/<feed>/*.parquet
and MERGEs them into the corresponding ``allianz_bronze`` Delta tables:

    weather             → allianz_bronze.weather_obs_raw
    earthquakes         → allianz_bronze.earthquakes_raw
    catastrophes        → allianz_bronze.catastrophe_events_raw
    noaa_alerts         → allianz_bronze.noaa_alerts_raw
    news                → allianz_bronze.news_raw

Each table is keyed for idempotency:

    weather_obs_raw         (observation_id)
    earthquakes_raw         (event_id)
    catastrophe_events_raw  (event_id)
    noaa_alerts_raw         (alert_id)
    news_raw                (news_id)

Usage:
    uv run --with "databricks-connect>=16.4,<17.0" src/ingest_external.py
"""
from __future__ import annotations

import argparse
import os

from databricks.connect import DatabricksSession
from pyspark.sql.utils import AnalysisException

DEFAULT_CATALOG = "serverless_stable_xhky6g_catalog"
DEFAULT_SCHEMA = "allianz_bronze"
DEFAULT_VOLUME = "landing"
DEFAULT_PROFILE = "fe-vm-fevm-serverless-stable-xhky6g"


# Each feed: (volume folder, target table, merge key column)
FEEDS = [
    ("weather",      "weather_obs_raw",        "observation_id"),
    ("earthquakes",  "earthquakes_raw",        "event_id"),
    ("catastrophes", "catastrophe_events_raw", "event_id"),
    ("noaa_alerts",  "noaa_alerts_raw",        "alert_id"),
    ("news",         "news_raw",               "news_id"),
]


def list_files(spark, volume_path: str) -> list[str]:
    """Return the list of parquet file paths under volume_path, or [] if absent."""
    try:
        # spark.sql LIST is the only Connect-safe way to list a volume
        rows = spark.sql(f"LIST '{volume_path}'").collect()
        return [r["path"] for r in rows if r["path"].endswith(".parquet")]
    except AnalysisException:
        return []


def merge_feed(spark, catalog: str, schema: str, volume: str,
               folder: str, table: str, key: str) -> int:
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/external/{folder}"
    files = list_files(spark, volume_path)
    if not files:
        print(f"  ⚠ no files at {volume_path}")
        return 0

    fq = f"{catalog}.{schema}.{table}"
    incoming = spark.read.parquet(volume_path)
    incoming_count = incoming.count()
    if incoming_count == 0:
        print(f"  ⚠ {fq}: 0 rows in landing files")
        return 0

    # Create the target table if missing using the incoming schema, then MERGE.
    if not spark.catalog.tableExists(fq):
        (incoming.limit(0)
         .write.format("delta").saveAsTable(fq))
        print(f"  + created table {fq}")

    incoming.createOrReplaceTempView("_src")
    spark.sql(f"""
        MERGE INTO {fq} t
        USING (SELECT * FROM _src) s
        ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    total = spark.table(fq).count()
    print(f"  → {fq}  merged {incoming_count:,} incoming, total now {total:,}")
    return incoming_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.environ.get("ALLIANZ_CATALOG", DEFAULT_CATALOG))
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--volume", default=DEFAULT_VOLUME)
    parser.add_argument("--profile", default=os.environ.get(
        "DATABRICKS_CONFIG_PROFILE", DEFAULT_PROFILE))
    args = parser.parse_args()

    print("=" * 70)
    print("BRONZE INGEST — UC volume → allianz_bronze.*_raw")
    print("=" * 70)
    print(f"Catalog : {args.catalog}")
    print(f"Schema  : {args.schema}")
    print(f"Volume  : /Volumes/{args.catalog}/{args.schema}/{args.volume}/external/")
    print()

    os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile
    spark = (DatabricksSession.builder
             .profile(args.profile)
             .serverless()
             .getOrCreate())
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

    total = 0
    for folder, table, key in FEEDS:
        total += merge_feed(spark, args.catalog, args.schema, args.volume,
                            folder, table, key)

    print()
    print(f"Done. {total:,} rows ingested across all feeds.")
    spark.stop()


if __name__ == "__main__":
    main()
