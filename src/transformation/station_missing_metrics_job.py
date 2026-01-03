from ..db.trino_client import TrinoClient
from .watermark import WatermarkStore
from ..utils.logging import get_logger

logger = get_logger(__name__)

class StationMissingMetricsJob:
    JOB_NAME = "station_missing_metrics"

    def __init__(self, trino: TrinoClient, watermark: WatermarkStore, schema: str, raw_table: str, metrics_table: str):
        self.trino = trino
        self.watermark = watermark
        self.schema = schema
        self.raw_table = raw_table
        self.metrics_table = metrics_table

    def run_incremental(self) -> None:
        last_ts = self.watermark.get(self.JOB_NAME)

        # Find max ingestion_ts in the raw table (for watermark update)
        max_ts_rows = self.trino.execute(f"""SELECT COALESCE(MAX(ingestion_ts), 0) FROM {self.schema}.{self.raw_table}""")
        max_ts = int(max_ts_rows[0][0]) if max_ts_rows else 0

        if max_ts <= last_ts:
            logger.info("No new data to transform.")
            return

        # Aggregate new records only
        # total_new, missing_new per station
        agg_sql = f"""
        WITH new_data AS (
            SELECT station_id, value
            FROM {self.schema}.{self.raw_table}
            WHERE ingestion_ts > {last_ts} AND ingestion_ts <= {max_ts}
        ),
        agg AS (
            SELECT
                station_id,
                COUNT(*) AS total_new,
                SUM(CASE WHEN value = 99999 THEN 1 ELSE 0 END) AS missing_new
            FROM new_data
            GROUP BY 1
        )
        SELECT station_id, total_new, missing_new
        FROM agg
        """

        new_rows = self.trino.execute(agg_sql)
        if not new_rows:
            self.watermark.upsert(self.JOB_NAME, max_ts)
            logger.info("No aggregatable new rows.")
            return

        # Merge strategy (SQL-only):
        # - Create a staging table from the aggregation
        # - Delete matching stations in metrics
        # - Insert updated totals as (old + new)
        self._merge(new_rows)

        self.watermark.upsert(self.JOB_NAME, max_ts)
        logger.info(f"Transformed {len(new_rows)} stations")

    def _merge(self, new_rows: list[tuple]) -> None:
        # Create staging using VALUES
        values = ",".join(f"('{sid}', {total}, {missing})" for sid, total, missing in new_rows)

        self.trino.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.station_metrics_staging (
            station_id VARCHAR,
            total_new BIGINT,
            missing_new BIGINT
        ) WITH (
            format='PARQUET',
            location = 's3://iceberg/{self.schema}/station_metrics_staging'
        )
        """)

        self.trino.execute(f"""DELETE FROM {self.schema}.station_metrics_staging""")
        self.trino.execute(f"""INSERT INTO {self.schema}.station_metrics_staging (station_id,total_new,missing_new) VALUES {values}""")

        # Rebuild metrics for affected stations
        self.trino.execute(f"""DROP TABLE IF EXISTS {self.schema}.station_metrics_updated""")

        # Build updated metrics by joining old metrics + new aggregation
        self.trino.execute(f"""
        CREATE TABLE {self.schema}.station_metrics_updated
        WITH (
            format='PARQUET',
            location = 's3://iceberg/{self.schema}/station_metrics_updated'
        )
        AS
        WITH old AS (
            SELECT station_id, total_observations, missing_observations
            FROM {self.schema}.{self.metrics_table}
        ),
        new AS (
            SELECT station_id, total_new, missing_new
            FROM {self.schema}.station_metrics_staging
        ),
        joined AS (
            SELECT
                COALESCE(o.station_id, n.station_id) AS station_id,
                COALESCE(o.total_observations, 0) + COALESCE(n.total_new, 0) AS total_observations,
                COALESCE(o.missing_observations, 0) + COALESCE(n.missing_new, 0) AS missing_observations
            FROM old o
            FULL OUTER JOIN new n
            ON o.station_id = n.station_id
            WHERE n.station_id IS NOT NULL  -- only stations impacted this run
        )
        SELECT
            station_id,
            total_observations,
            missing_observations,
            CASE WHEN total_observations = 0 THEN 0.0
                 ELSE (missing_observations * 100.0 / total_observations) END AS missing_percentage,
            CAST(to_unixtime(current_timestamp) * 1000 AS BIGINT) AS last_updated_ts
        FROM joined
        """)

        # Remove old rows for impacted stations, then insert updated ones
        self.trino.execute(f"""
        DELETE FROM {self.schema}.{self.metrics_table}
        WHERE station_id IN (SELECT station_id FROM {self.schema}.station_metrics_staging)
        """)

        self.trino.execute(f"""
        INSERT INTO {self.schema}.{self.metrics_table}
        (station_id,total_observations,missing_observations,missing_percentage,last_updated_ts)
        SELECT station_id,total_observations,missing_observations,missing_percentage,last_updated_ts
        FROM {self.schema}.station_metrics_updated
        """)

        self.trino.execute(f"""DROP TABLE IF EXISTS {self.schema}.station_metrics_updated""")

