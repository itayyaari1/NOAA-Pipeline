from ..db.trino_client import TrinoClient
from ..utils.logging import get_logger

logger = get_logger(__name__)

class IcebergMaintenanceJob:
    def __init__(self, trino: TrinoClient, schema: str, raw_table: str):
        self.trino = trino
        self.schema = schema
        self.raw_table = raw_table

    def run(self) -> None:
        fqtn = f"{self.schema}.{self.raw_table}"

        # 1) Expire snapshots (keep recent ones)
        try:
            self.trino.execute(f"""
            CALL system.expire_snapshots('{fqtn}', retention_threshold => INTERVAL '1' DAY)
            """)
        except Exception:
            logger.debug("expire_snapshots not supported in this Trino version")

        # 2) Remove orphan files
        try:
            self.trino.execute(f"""
            CALL system.remove_orphan_files('{fqtn}', retention_threshold => INTERVAL '1' DAY)
            """)
        except Exception:
            logger.debug("remove_orphan_files not supported in this Trino version")

        # 3) Optional: rewrite data files (compaction)
        try:
            self.trino.execute(f"""
            CALL system.rewrite_data_files('{fqtn}')
            """)
        except Exception:
            logger.debug("rewrite_data_files not supported in this Trino version")

        logger.info("Maintenance completed")

