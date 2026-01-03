from src.config import Settings
from src.db.trino_client import TrinoClient
from src.db import ddl

from src.ingestion.noaa_client import NOAAClient
from src.ingestion.normalizer import DataNormalizer
from src.ingestion.validator import DataValidator
from src.ingestion.ingest_job import IngestJob

from src.transformation.watermark import WatermarkStore
from src.transformation.station_missing_metrics_job import StationMissingMetricsJob

from src.maintenance.maintenance_job import IcebergMaintenanceJob

def init():
    s = Settings()
    trino = TrinoClient(s.trino_host, s.trino_port, s.trino_user, s.trino_catalog, s.trino_schema)
    trino.execute(ddl.create_schema(s.trino_schema))
    trino.execute(ddl.create_raw_table(s.trino_schema, s.raw_table))
    trino.execute(ddl.create_metrics_table(s.trino_schema, s.metrics_table))
    trino.execute(ddl.create_watermark_table(s.trino_schema, s.watermark_table))

def ingest():
    s = Settings()
    trino = TrinoClient(s.trino_host, s.trino_port, s.trino_user, s.trino_catalog, s.trino_schema)

    noaa = NOAAClient(s.noaa_base_url, s.noaa_token, timeout_s=s.request_timeout_s)
    normalizer = DataNormalizer()
    validator = DataValidator()

    job = IngestJob(noaa, normalizer, validator, trino, s.trino_schema, s.raw_table)
    job.run(s.dataset_id, s.start_date, s.end_date, limit=s.page_limit)

def transform():
    s = Settings()
    trino = TrinoClient(s.trino_host, s.trino_port, s.trino_user, s.trino_catalog, s.trino_schema)

    watermark = WatermarkStore(trino, s.trino_schema, s.watermark_table)
    job = StationMissingMetricsJob(trino, watermark, s.trino_schema, s.raw_table, s.metrics_table)
    job.run_incremental()

def maintain():
    s = Settings()
    trino = TrinoClient(s.trino_host, s.trino_port, s.trino_user, s.trino_catalog, s.trino_schema)

    job = IcebergMaintenanceJob(trino, s.trino_schema, s.raw_table)
    job.run()

def main():
    init()
    ingest()
    transform()
    maintain()

if __name__ == "__main__":
    main()
