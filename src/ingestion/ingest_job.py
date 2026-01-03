import json
from typing import Any, Dict, List
from .noaa_client import NOAAClient
from .normalizer import DataNormalizer
from .validator import DataValidator
from ..db.trino_client import TrinoClient
from ..utils.logging import get_logger

logger = get_logger(__name__)

class IngestJob:
    def __init__(
        self,
        noaa: NOAAClient,
        normalizer: DataNormalizer,
        validator: DataValidator,
        trino: TrinoClient,
        schema: str,
        raw_table: str,
    ):
        self.noaa = noaa
        self.normalizer = normalizer
        self.validator = validator
        self.trino = trino
        self.schema = schema
        self.raw_table = raw_table

    def run(self, dataset_id: str, start_date: str, end_date: str, limit: int = 1000) -> None:
        records = self.noaa.fetch_month(dataset_id, start_date, end_date, limit=limit)
        logger.info(f"Fetched {len(records)} records from NOAA API")

        good: List[Dict[str, Any]] = []
        bad_count = 0

        for r in records:
            n = self.normalizer.normalize(r)
            ok, err = self.validator.validate(n)
            if ok:
                good.append(n)
            else:
                bad_count += 1

        if not good:
            logger.warning("No valid records to ingest.")
            return

        # Insert in batches to avoid huge SQL statements
        batch_size = 500
        total_batches = (len(good) + batch_size - 1) // batch_size
        logger.info(f"Inserting {len(good)} records in {total_batches} batch(es)...")
        
        for i in range(0, len(good), batch_size):
            batch = good[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            self._insert_batch(batch)
            logger.info(f"  Batch {batch_num}/{total_batches} inserted ({len(batch)} records)")

        logger.info(f"Ingested {len(good)} records ({bad_count} invalid skipped)")

    def _insert_batch(self, batch: List[Dict[str, Any]]) -> None:
        values_sql = []
        for r in batch:
            station = self._sql_str(r["station_id"])
            datatype = self._sql_str(r["datatype"])
            value = int(r["value"]) if r["value"] is not None else "NULL"
            event_ms = int(r["event_time_ms"]) if r["event_time_ms"] is not None else "NULL"
            ingestion_ts = int(r["ingestion_ts"])

            # attributes: ARRAY(VARCHAR)
            attrs = r.get("attributes") or []
            attrs_sql = "ARRAY[" + ",".join(self._sql_str(a) for a in attrs) + "]"

            # source_record VARCHAR (store as JSON string)
            src_json = self._sql_str(json.dumps(r["source_record"], ensure_ascii=False))

            values_sql.append(f"({station},{datatype},{value},{event_ms},{attrs_sql},{src_json},{ingestion_ts})")

        sql = f"""
        INSERT INTO {self.schema}.{self.raw_table}
        (station_id, datatype, value, event_time_ms, attributes, source_record, ingestion_ts)
        VALUES {",".join(values_sql)}
        """
        self.trino.execute(sql)

    def _sql_str(self, s: str) -> str:
        if s is None:
            return "NULL"
        escaped = str(s).replace("'", "''")
        return f"'{escaped}'"

