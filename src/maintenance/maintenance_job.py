from ..db.trino_client import TrinoClient

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
        except Exception as e:
            print(f"expire_snapshots not supported or failed: {e}")

        # 2) Remove orphan files
        try:
            self.trino.execute(f"""
            CALL system.remove_orphan_files('{fqtn}', retention_threshold => INTERVAL '1' DAY)
            """)
        except Exception as e:
            print(f"remove_orphan_files not supported or failed: {e}")

        # 3) Optional: rewrite data files (compaction)
        # If not supported in your Trino version, comment out.
        try:
            self.trino.execute(f"""
            CALL system.rewrite_data_files('{fqtn}')
            """)
        except Exception as e:
            print(f"rewrite_data_files not supported or failed: {e}")

        print("Maintenance completed.")

