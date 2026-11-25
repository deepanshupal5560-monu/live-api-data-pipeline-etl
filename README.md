# End-to-End Live API Ingestion & Incremental ETL Pipeline

An automated Python and SQL data engineering pipeline that ingests real-time telemetry from a public REST API, enforces schema and range data-quality checks, logs pipeline execution metrics, and performs deduplicated incremental loads into a SQLite data warehouse.

## Architecture & Key Features
1. **Live API Extraction:** Fetches real-time JSON telemetry across global tech hubs (`New York`, `London`, `Tokyo`, `Berlin`, `Bengaluru`, `Noida`) with timeout and exception handling.
2. **Data Quality & Transformation:** Validates null thresholds and physical range bounds, derives Fahrenheit conversions, and classifies climate states.
3. **Incremental Loading:** Uses composite Primary Keys `(city, api_timestamp)` with `INSERT OR IGNORE` to guarantee idempotent, duplicate-free ingestion into `global_weather_warehouse.db`.
4. **Observability:** Writes structured execution and error logs to `pipeline.log` for rapid job monitoring and troubleshooting.

## How to Run
    pip install -r requirements.txt
    python api_etl_pipeline.py