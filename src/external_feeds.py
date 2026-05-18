"""
Pulls real-time external feeds for Allianz risk-correlation pipeline:

  • Weather observations from Open-Meteo (current weather per CRESTA zone)
  • Storm events from NOAA's SWDI/Storm-Events public archive (latest CSV)
  • Catastrophe events from GDACS (Global Disaster Alert) RSS
  • Earthquake events from USGS (last 30 days)
  • Humanitarian / risk news from ReliefWeb RSS

Writes results to UC bronze schema, where the DLT pipeline picks them up.

Designed to run as a Databricks job task (no Spark required for the fetch
step; we just use Connect to write the resulting DataFrames).

Usage:
    uv run --with polars --with httpx --with feedparser \
        --with "databricks-connect>=16.4,<17.0" \
        src/external_feeds.py
"""
from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone

import httpx
import polars as pl

DEFAULT_CATALOG = "serverless_stable_xhky6g_catalog"
DEFAULT_SCHEMA = "allianz_ext"

# Sample of CRESTA-like locations covering high-exposure US cities
LOCATIONS = [
    ("Miami, FL",     25.7617, -80.1918, "USA-FL-1"),
    ("Houston, TX",   29.7604, -95.3698, "USA-TX-2"),
    ("New Orleans, LA", 29.9511, -90.0715, "USA-LA-1"),
    ("Tampa, FL",     27.9506, -82.4572, "USA-FL-2"),
    ("Charleston, SC", 32.7765, -79.9311, "USA-SC-1"),
    ("Norfolk, VA",   36.8508, -76.2859, "USA-VA-1"),
    ("Los Angeles, CA", 34.0522, -118.2437, "USA-CA-1"),
    ("San Francisco, CA", 37.7749, -122.4194, "USA-CA-2"),
    ("Seattle, WA",   47.6062, -122.3321, "USA-WA-1"),
    ("New York, NY",  40.7128, -74.0060,  "USA-NY-1"),
    ("Chicago, IL",   41.8781, -87.6298,  "USA-IL-1"),
    ("Boston, MA",    42.3601, -71.0589,  "USA-MA-1"),
    ("Denver, CO",    39.7392, -104.9903, "USA-CO-1"),
    ("Phoenix, AZ",   33.4484, -112.0740, "USA-AZ-1"),
    ("Atlanta, GA",   33.7490, -84.3880,  "USA-GA-1"),
]


# --------------------------------------------------------------------------
# Fetchers
# --------------------------------------------------------------------------
def fetch_weather() -> pl.DataFrame:
    """Open-Meteo: current weather conditions per CRESTA city."""
    rows = []
    fetched_at = datetime.now(timezone.utc).isoformat()
    with httpx.Client(timeout=15.0) as client:
        for city, lat, lon, cresta in LOCATIONS:
            try:
                r = client.get(
                    "https://api.open-meteo.com/v1/forecast",
                    params={
                        "latitude": lat, "longitude": lon,
                        "current": ",".join([
                            "temperature_2m", "wind_speed_10m", "wind_gusts_10m",
                            "precipitation", "rain", "weather_code", "pressure_msl",
                            "relative_humidity_2m",
                        ]),
                        "wind_speed_unit": "mph",
                        "temperature_unit": "fahrenheit",
                        "precipitation_unit": "inch",
                        "timezone": "UTC",
                    },
                )
                if r.status_code != 200:
                    continue
                cur = r.json().get("current", {})
                rows.append({
                    "observation_id": str(uuid.uuid4()),
                    "city": city,
                    "cresta_zone": cresta,
                    "latitude": lat,
                    "longitude": lon,
                    "observed_time_utc": cur.get("time"),
                    "temperature_f": cur.get("temperature_2m"),
                    "wind_speed_mph": cur.get("wind_speed_10m"),
                    "wind_gust_mph": cur.get("wind_gusts_10m"),
                    "precipitation_in": cur.get("precipitation"),
                    "rain_in": cur.get("rain"),
                    "weather_code": cur.get("weather_code"),
                    "pressure_mb": cur.get("pressure_msl"),
                    "humidity_pct": cur.get("relative_humidity_2m"),
                    "fetched_at_utc": fetched_at,
                    "source": "open-meteo",
                })
            except Exception as e:
                print(f"  ! weather fetch failed for {city}: {e}")
    return pl.DataFrame(rows)


def fetch_usgs_earthquakes() -> pl.DataFrame:
    """USGS: significant earthquakes in the last 30 days (worldwide)."""
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson"
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    try:
        with httpx.Client(timeout=20.0) as client:
            r = client.get(url)
            if r.status_code == 200:
                features = r.json().get("features", [])
                for f in features:
                    p = f.get("properties", {})
                    g = f.get("geometry", {})
                    coords = g.get("coordinates", [None, None, None])
                    rows.append({
                        "event_id": f.get("id"),
                        "event_type": "Earthquake",
                        "magnitude": p.get("mag"),
                        "place": p.get("place"),
                        "longitude": coords[0],
                        "latitude": coords[1],
                        "depth_km": coords[2],
                        "event_time_utc": datetime.fromtimestamp(
                            (p.get("time") or 0) / 1000.0, tz=timezone.utc
                        ).isoformat(),
                        "url": p.get("url"),
                        "severity": "high" if (p.get("mag") or 0) >= 6.5 else "moderate",
                        "fetched_at_utc": fetched_at,
                        "source": "usgs",
                    })
    except Exception as e:
        print(f"  ! USGS fetch failed: {e}")
    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={
        "event_id": pl.Utf8, "event_type": pl.Utf8, "magnitude": pl.Float64,
        "place": pl.Utf8, "longitude": pl.Float64, "latitude": pl.Float64,
        "depth_km": pl.Float64, "event_time_utc": pl.Utf8, "url": pl.Utf8,
        "severity": pl.Utf8, "fetched_at_utc": pl.Utf8, "source": pl.Utf8,
    })


def fetch_gdacs_disasters() -> pl.DataFrame:
    """GDACS: global disaster RSS (parsed minimally without feedparser dep)."""
    import re
    url = "https://www.gdacs.org/xml/rss.xml"
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    try:
        with httpx.Client(timeout=20.0, follow_redirects=True) as client:
            r = client.get(url)
            if r.status_code != 200:
                return pl.DataFrame(schema=_gdacs_schema())
            text = r.text
            items = re.findall(r"<item>(.*?)</item>", text, re.DOTALL)
            for it in items[:200]:
                title = (re.search(r"<title>(.*?)</title>", it, re.DOTALL) or [None, ""])
                desc  = (re.search(r"<description>(.*?)</description>", it, re.DOTALL) or [None, ""])
                pub   = (re.search(r"<pubDate>(.*?)</pubDate>", it, re.DOTALL) or [None, ""])
                lat   = (re.search(r"<geo:lat>(.*?)</geo:lat>", it, re.DOTALL) or [None, None])
                lon   = (re.search(r"<geo:long>(.*?)</geo:long>", it, re.DOTALL) or [None, None])
                link  = (re.search(r"<link>(.*?)</link>", it, re.DOTALL) or [None, ""])
                t = title[1].strip() if title[1] else ""
                rows.append({
                    "event_id": str(uuid.uuid4()),
                    "title": t,
                    "description": (desc[1] or "").strip()[:500],
                    "event_type": _classify_gdacs(t),
                    "severity": "high" if "RED" in t.upper() else "moderate" if "ORANGE" in t.upper() else "low",
                    "latitude": float(lat[1]) if lat[1] else None,
                    "longitude": float(lon[1]) if lon[1] else None,
                    "event_time_utc": (pub[1] or "").strip(),
                    "link": (link[1] or "").strip(),
                    "fetched_at_utc": fetched_at,
                    "source": "gdacs",
                })
    except Exception as e:
        print(f"  ! GDACS fetch failed: {e}")
    if not rows:
        return pl.DataFrame(schema=_gdacs_schema())
    return pl.DataFrame(rows)


def _gdacs_schema():
    return {
        "event_id": pl.Utf8, "title": pl.Utf8, "description": pl.Utf8,
        "event_type": pl.Utf8, "severity": pl.Utf8,
        "latitude": pl.Float64, "longitude": pl.Float64,
        "event_time_utc": pl.Utf8, "link": pl.Utf8,
        "fetched_at_utc": pl.Utf8, "source": pl.Utf8,
    }


def _classify_gdacs(title: str) -> str:
    t = title.upper()
    if "TC " in t or "CYCLONE" in t or "HURRICANE" in t: return "TropicalCyclone"
    if "EQ " in t or "EARTHQUAKE" in t:                  return "Earthquake"
    if "FL " in t or "FLOOD" in t:                       return "Flood"
    if "DR " in t or "DROUGHT" in t:                     return "Drought"
    if "WF " in t or "WILDFIRE" in t:                    return "Wildfire"
    if "VO " in t or "VOLCAN" in t:                      return "Volcanic"
    return "Other"


def fetch_reliefweb_news() -> pl.DataFrame:
    """ReliefWeb headlines (humanitarian risk events)."""
    import re
    url = "https://reliefweb.int/updates/rss.xml"
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    try:
        with httpx.Client(timeout=20.0, follow_redirects=True,
                          headers={"User-Agent": "Allianz-Demo/1.0"}) as client:
            r = client.get(url)
            if r.status_code != 200:
                return pl.DataFrame(schema=_news_schema())
            items = re.findall(r"<item>(.*?)</item>", r.text, re.DOTALL)
            for it in items[:100]:
                title = (re.search(r"<title>(.*?)</title>", it, re.DOTALL) or [None, ""])[1].strip()
                desc = (re.search(r"<description>(.*?)</description>", it, re.DOTALL) or [None, ""])[1].strip()
                pub = (re.search(r"<pubDate>(.*?)</pubDate>", it, re.DOTALL) or [None, ""])[1].strip()
                link = (re.search(r"<link>(.*?)</link>", it, re.DOTALL) or [None, ""])[1].strip()
                rows.append({
                    "news_id": str(uuid.uuid4()),
                    "title": title,
                    "summary": desc[:500],
                    "published_utc": pub,
                    "link": link,
                    "category": _classify_news(title),
                    "fetched_at_utc": fetched_at,
                    "source": "reliefweb",
                })
    except Exception as e:
        print(f"  ! ReliefWeb fetch failed: {e}")
    if not rows:
        return pl.DataFrame(schema=_news_schema())
    return pl.DataFrame(rows)


def _news_schema():
    return {
        "news_id": pl.Utf8, "title": pl.Utf8, "summary": pl.Utf8,
        "published_utc": pl.Utf8, "link": pl.Utf8, "category": pl.Utf8,
        "fetched_at_utc": pl.Utf8, "source": pl.Utf8,
    }


def _classify_news(title: str) -> str:
    t = (title or "").lower()
    if any(k in t for k in ("flood", "hurricane", "cyclone", "tornado", "earthquake", "wildfire", "typhoon")):
        return "Catastrophe"
    if any(k in t for k in ("cyber", "ransomware", "breach")):
        return "Cyber"
    if any(k in t for k in ("epidemic", "outbreak", "covid")):
        return "Health"
    if any(k in t for k in ("conflict", "war", "violence")):
        return "Conflict"
    return "General"


def fetch_noaa_storm_summary() -> pl.DataFrame:
    """NOAA active alerts (lightweight summary endpoint)."""
    url = "https://api.weather.gov/alerts/active?status=actual&message_type=alert"
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    try:
        with httpx.Client(timeout=20.0,
                          headers={"User-Agent": "Allianz-Demo/1.0", "Accept": "application/geo+json"}) as client:
            r = client.get(url)
            if r.status_code != 200:
                return pl.DataFrame(schema=_storm_schema())
            for feat in r.json().get("features", [])[:300]:
                p = feat.get("properties", {})
                rows.append({
                    "alert_id": feat.get("id"),
                    "event_type": p.get("event"),
                    "severity": p.get("severity"),
                    "certainty": p.get("certainty"),
                    "urgency": p.get("urgency"),
                    "area_desc": p.get("areaDesc"),
                    "headline": p.get("headline"),
                    "sent_utc": p.get("sent"),
                    "effective_utc": p.get("effective"),
                    "expires_utc": p.get("expires"),
                    "fetched_at_utc": fetched_at,
                    "source": "noaa-nws",
                })
    except Exception as e:
        print(f"  ! NOAA fetch failed: {e}")
    if not rows:
        return pl.DataFrame(schema=_storm_schema())
    return pl.DataFrame(rows)


def _storm_schema():
    return {
        "alert_id": pl.Utf8, "event_type": pl.Utf8, "severity": pl.Utf8,
        "certainty": pl.Utf8, "urgency": pl.Utf8, "area_desc": pl.Utf8,
        "headline": pl.Utf8, "sent_utc": pl.Utf8, "effective_utc": pl.Utf8,
        "expires_utc": pl.Utf8, "fetched_at_utc": pl.Utf8, "source": pl.Utf8,
    }


# --------------------------------------------------------------------------
# Write
# --------------------------------------------------------------------------
def write_uc(spark, df: pl.DataFrame, catalog: str, schema: str, table: str):
    if df.is_empty():
        print(f"  ⚠ skipping empty {table}")
        return
    print(f"  → {catalog}.{schema}.{table}  ({df.height:,} rows)")
    spark_df = spark.createDataFrame(df.to_pandas())
    (spark_df.write.format("delta")
     .mode("append")
     .option("mergeSchema", "true")
     .saveAsTable(f"{catalog}.{schema}.{table}"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.environ.get("ALLIANZ_CATALOG", DEFAULT_CATALOG))
    parser.add_argument("--schema", default=os.environ.get("ALLIANZ_EXT_SCHEMA", DEFAULT_SCHEMA))
    parser.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE",
                                                            "fe-vm-fevm-serverless-stable-xhky6g"))
    parser.add_argument("--local-only", action="store_true")
    args = parser.parse_args()

    print("=" * 70)
    print("ALLIANZ EXTERNAL FEEDS — weather / catastrophe / news")
    print("=" * 70)

    print("Fetching Open-Meteo weather…");      weather = fetch_weather()
    print(f"  {weather.height} obs")
    print("Fetching USGS earthquakes…");        eq = fetch_usgs_earthquakes()
    print(f"  {eq.height} events")
    print("Fetching GDACS disasters…");         gdacs = fetch_gdacs_disasters()
    print(f"  {gdacs.height} events")
    print("Fetching NOAA alerts…");             noaa = fetch_noaa_storm_summary()
    print(f"  {noaa.height} alerts")
    print("Fetching ReliefWeb news…");          news = fetch_reliefweb_news()
    print(f"  {news.height} stories")

    tables = {
        "weather_obs_raw": weather,
        "earthquakes_raw": eq,
        "catastrophe_events_raw": gdacs,
        "noaa_alerts_raw": noaa,
        "news_raw": news,
    }

    if args.local_only:
        os.makedirs("output", exist_ok=True)
        for n, df in tables.items():
            if not df.is_empty():
                df.write_parquet(f"output/{n}.parquet")
                print(f"  → output/{n}.parquet  ({df.height:,} rows)")
        return

    from databricks.connect import DatabricksSession
    os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile
    spark = (DatabricksSession.builder
             .profile(args.profile)
             .serverless()
             .getOrCreate())
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    for n, df in tables.items():
        write_uc(spark, df, args.catalog, args.schema, n)
    spark.stop()


if __name__ == "__main__":
    main()
