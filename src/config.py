from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    # NOAA
    noaa_token: str = os.environ.get("NOAA_API_TOKEN", "")
    noaa_base_url: str = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    dataset_id: str = "PRECIP_15"

    # Choose a month with data (e.g., 2010-01)
    start_date: str = os.environ.get("START_DATE", "2010-01-01")
    end_date: str = os.environ.get("END_DATE", "2010-01-31")

    # Trino
    trino_host: str = os.environ.get("TRINO_HOST", "localhost")
    trino_port: int = int(os.environ.get("TRINO_PORT", "8080"))
    trino_user: str = os.environ.get("TRINO_USER", "pipeline")
    trino_catalog: str = os.environ.get("TRINO_CATALOG", "iceberg")
    trino_schema: str = os.environ.get("TRINO_SCHEMA", "home_assignment")

    # Tables
    raw_table: str = os.environ.get("RAW_TABLE", "precipitation_raw")
    metrics_table: str = os.environ.get("METRICS_TABLE", "station_missing_metrics")
    watermark_table: str = os.environ.get("WATERMARK_TABLE", "pipeline_watermark")

    # Pipeline options
    page_limit: int = int(os.environ.get("NOAA_PAGE_LIMIT", "1000"))  # NOAA supports pagination
    request_timeout_s: int = int(os.environ.get("REQUEST_TIMEOUT_S", "30"))

