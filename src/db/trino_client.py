from trino.dbapi import connect
from trino.auth import BasicAuthentication
from typing import Any, Iterable, Optional
from . import ddl

class TrinoClient:
    def __init__(self, host: str, port: int, user: str, catalog: str, schema: str):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema

    def execute(self, sql: str) -> list[tuple]:
        conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
        )
        cur = conn.cursor()
        cur.execute(sql)
        try:
            rows = cur.fetchall()
        except Exception:
            rows = []
        finally:
            cur.close()
            conn.close()
        return rows

