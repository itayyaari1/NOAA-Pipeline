import time
from typing import Optional
from ..db.trino_client import TrinoClient

def now_ms() -> int:
    return int(time.time() * 1000)

class WatermarkStore:
    def __init__(self, trino: TrinoClient, schema: str, table: str):
        self.trino = trino
        self.schema = schema
        self.table = table

    def get(self, job_name: str) -> int:
        sql = f"""
        SELECT COALESCE(MAX(last_processed_ingestion_ts), 0)
        FROM {self.schema}.{self.table}
        WHERE job_name = '{job_name}'
        """
        rows = self.trino.execute(sql)
        return int(rows[0][0]) if rows else 0

    def upsert(self, job_name: str, last_processed_ingestion_ts: int) -> None:
        # simplest: delete + insert (ok for small table)
        self.trino.execute(f"""DELETE FROM {self.schema}.{self.table} WHERE job_name = '{job_name}'""")
        self.trino.execute(
            f"""INSERT INTO {self.schema}.{self.table}
            (job_name, last_processed_ingestion_ts, updated_ts)
            VALUES ('{job_name}', {last_processed_ingestion_ts}, {now_ms()})
            """
        )

