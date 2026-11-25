from datetime import datetime, timezone
import logging
import sqlite3
import pandas as pd
import requests

DB_FILE = "global_weather_warehouse.db"
EXPORT_CSV = "processed_city_metrics.csv"
LOG_FILE = "pipeline.log"

# Configure structured job logging for monitoring & troubleshooting
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Target global & Indian tech hubs (City, Country, Latitude, Longitude)
LOCATIONS = [
    ("New York", "USA", 40.7128, -74.0060),
    ("London", "UK", 51.5072, -0.1276),
    ("Tokyo", "Japan", 35.6762, 139.6503),
    ("Berlin", "Germany", 52.5200, 13.4050),
    ("Bengaluru", "India", 12.9716, 77.5946),
    ("Noida", "India", 28.5355, 77.3910),
]


def extract_live_api_data():
  """Extracts real-time weather metrics from Open-Meteo REST API."""
  raw_records = []
  logging.info("Starting API extraction job for %d locations.", len(LOCATIONS))

  for city, country, lat, lon in LOCATIONS:
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
    )
    try:
      response = requests.get(url, timeout=10)
      response.raise_for_status()
      payload = response.json()
      current = payload.get("current", {})

      raw_records.append({
          "city": city,
          "country": country,
          "latitude": lat,
          "longitude": lon,
          "temperature_c": current.get("temperature_2m"), 
          "humidity_pct": current.get("relative_humidity_2m"),
          "wind_speed_kmh": current.get("wind_speed_10m"),
          "api_timestamp": current.get("time"),
          "ingested_at_utc": datetime.now(timezone.utc).strftime(
              "%Y-%m-%d %H:%M:%S"
          ),
      })
      logging.info("Successfully extracted data for %s, %s.", city, country)
    except requests.RequestException as exc:
      logging.error("API extraction failed for %s: %s", city, exc)

  print(
      f"[STEP 1] Extracted live API records for {len(raw_records)} cities."
  )
  return pd.DataFrame(raw_records)


def transform_and_validate(df):
  """Cleans nested API fields, derives Fahrenheit & comfort index, and enforces data quality rules."""
  if df.empty:
    logging.warning("No records extracted. Aborting transformation.")
    return df

  # Drop rows with null critical telemetry
  initial_count = len(df)
  df = df.dropna(
      subset=["temperature_c", "humidity_pct", "wind_speed_kmh", "api_timestamp"]
  ).copy()

  # Range validation (Data Quality check)
  df = df[(df["temperature_c"] >= -60) & (df["temperature_c"] <= 60)]
  df = df[(df["humidity_pct"] >= 0) & (df["humidity_pct"] <= 100)]

  # Derived transformations
  df["temperature_f"] = round((df["temperature_c"] * 9 / 5) + 32, 2)
  df["climate_status"] = df["temperature_c"].apply(
      lambda t: "Cold" if t < 15 else ("Moderate" if t <= 28 else "Warm")
  )

  dropped = initial_count - len(df)
  logging.info(
      "Transformation complete. Valid: %d | Dropped: %d", len(df), dropped
  )
  print(
      f"[STEP 2] Data Quality Check Passed -> Valid: {len(df)} | Dropped:"
      f" {dropped}"
  )
  return df


def load_incremental_to_sqlite(df):
  """Loads validated API records into SQLite warehouse with composite primary key deduplication."""
  if df.empty:
    return

  conn = sqlite3.connect(DB_FILE)
  cursor = conn.cursor()

  cursor.execute("""
        CREATE TABLE IF NOT EXISTS city_weather_telemetry (
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            api_timestamp TEXT NOT NULL,
            temperature_c REAL NOT NULL,
            temperature_f REAL NOT NULL,
            humidity_pct REAL NOT NULL,
            wind_speed_kmh REAL NOT NULL,
            climate_status TEXT NOT NULL,
            ingested_at_utc TEXT NOT NULL,
            PRIMARY KEY (city, api_timestamp)
        )
    """)

  inserted = 0
  for _, row in df.iterrows():
    cursor.execute(
        """
            INSERT OR IGNORE INTO city_weather_telemetry (
                city, country, api_timestamp, temperature_c, temperature_f,
                humidity_pct, wind_speed_kmh, climate_status, ingested_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["city"],
            row["country"],
            row["api_timestamp"],
            row["temperature_c"],
            row["temperature_f"],
            row["humidity_pct"],
            row["wind_speed_kmh"],
            row["climate_status"],
            row["ingested_at_utc"],
        ),
    )
    inserted += cursor.rowcount

  conn.commit()
  df.to_csv(EXPORT_CSV, index=False)
  logging.info("Loaded %d new rows into %s.", inserted, DB_FILE)
  print(
      f"[STEP 3] Incremental Load Complete -> New rows inserted: {inserted}"
      f" into {DB_FILE}"
  )

  print("\n--- LIVE GLOBAL TELEMETRY WAREHOUSE REPORT ---")
  summary_df = pd.read_sql_query(
      """
        SELECT 
            city AS City,
            country AS Country,
            temperature_c AS Temp_C,
            temperature_f AS Temp_F,
            humidity_pct AS Humidity_Pct,
            wind_speed_kmh AS Wind_KMH,
            climate_status AS Status
        FROM city_weather_telemetry
        ORDER BY temperature_c DESC;
    """,
      conn,
  )
  print(summary_df.to_string(index=False))
  conn.close()


if __name__ == "__main__":
  raw_df = extract_live_api_data()
  clean_df = transform_and_validate(raw_df)
  load_incremental_to_sqlite(clean_df)