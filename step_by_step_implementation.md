# Step-by-Step Implementation (Cursor-Ready)
## Data Engineer Home Assignment — Ingestion → Iceberg → Transformation → Maintenance

This guide is intentionally **actionable**: each step includes **exact files to create**, **code templates**, and **SQL sanity checks**. The goal is that Cursor can follow this document and implement the project end-to-end.

---

## 0) Quick Summary of What You Must Deliver

You will deliver a Git repo containing:
- Python pipeline code (OOP, modular)
- **One Dockerfile** that runs `src/main.py`
- A simple architecture diagram (can be ASCII/PNG)
- SQL queries proving:
  1) ingestion is correct
  2) transformation result is valid
- README with run instructions

The pipeline must:
1) Fetch **one full month** of NOAA `PRECIP_15` data (year with enough coverage, e.g., 2010)
2) Apply **3 required normalizations**
   - Expand concatenated strings → arrays
   - Convert timestamps → **epoch ms**
   - Add `ingestion_ts` column
3) **Strictly validate** data according to the public schema (use Pydantic)
4) Write normalized data **directly to Apache Iceberg** stored on **MinIO (S3 API)**
5) Transformation (incremental): per station missing % where `value = 99999`
6) Maintenance: snapshot management / orphan file cleanup / optimize files

---

## 1) Start the Local Datalake (Trino + Iceberg REST + MinIO)

From repo root:
```bash
cd datalake/trino
docker compose up -d
```

Confirm services:
- Trino UI: http://localhost:8080
- Iceberg REST: http://localhost:8183
- MinIO: http://localhost:9000

> The provided Trino `iceberg.properties` is configured for an Iceberg REST catalog and MinIO.
> It uses a warehouse: `s3://iceberg/` (bucket path).

---

## 2) Python: Dependencies and Project Layout

### 2.1 Create a Python package layout

Create these files/folders:
```
src/
  main.py
  config.py
  db/
    trino_client.py
    ddl.py
  ingestion/
    noaa_client.py
    normalizer.py
    schema.py
    validator.py
    ingest_job.py
  transformation/
    watermark.py
    station_missing_metrics_job.py
  maintenance/
    maintenance_job.py
  utils/
    time.py
    logging.py
requirements.txt
Dockerfile
sql/
  sanity_ingestion.sql
  sanity_transformation.sql
ARCHITECTURE.md
README.md
```

### 2.2 requirements.txt (suggested)
Create `requirements.txt`:
```txt
requests==2.32.3
pydantic==2.8.2
python-dateutil==2.9.0.post0
trino==0.333.0
tenacity==8.5.0
```

> Why Trino client? It’s the simplest way to:
> - create Iceberg tables (via Trino Iceberg connector)
> - insert rows into Iceberg using SQL
> - run maintenance procedures via SQL CALLs

---

## 3) Configuration (src/config.py)

Create `src/config.py`:

```python
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
```

---

## 4) Trino Client (src/db/trino_client.py)

Create a small reusable client to run SQL:

```python
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
```

> If your Trino requires auth, add BasicAuthentication. In local compose it usually does not.

---

## 5) DDL: Initialize the Iceberg Schema and Tables (src/db/ddl.py)

Create `src/db/ddl.py` with SQL strings.

### 5.1 Create schema
```python
def create_schema(schema: str) -> str:
    return f"""CREATE SCHEMA IF NOT EXISTS {schema}"""
```

### 5.2 Create raw Iceberg table

You will store normalized data in an Iceberg table. NOAA response (commonly) includes:
- `date` (timestamp string)
- `station` (identifier)
- `datatype`
- `value` (numeric)
- `attributes` (sometimes concatenated values)

Because NOAA fields can vary, keep table flexible but typed. Use:
- `event_time_ms BIGINT`
- `ingestion_ts BIGINT`
- `attributes ARRAY(VARCHAR)`

```python
def create_raw_table(schema: str, table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        station_id VARCHAR,
        datatype VARCHAR,
        value BIGINT,
        event_time_ms BIGINT,
        attributes ARRAY(VARCHAR),
        source_record JSON,
        ingestion_ts BIGINT
    )
    WITH (
        format = 'PARQUET'
    )
    """
```

> `source_record JSON` keeps the original payload (useful for debugging + “strict validation” evidence).

### 5.3 Create metrics table
```python
def create_metrics_table(schema: str, table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        station_id VARCHAR,
        total_observations BIGINT,
        missing_observations BIGINT,
        missing_percentage DOUBLE,
        last_updated_ts BIGINT
    )
    WITH (format='PARQUET')
    """
```

### 5.4 Create watermark table
This enables **incremental transformation**.

```python
def create_watermark_table(schema: str, table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        job_name VARCHAR,
        last_processed_ingestion_ts BIGINT,
        updated_ts BIGINT
    )
    WITH (format='PARQUET')
    """
```

---

## 6) NOAA API Client (src/ingestion/noaa_client.py)

Create `src/ingestion/noaa_client.py`

Key requirements:
- datasetid = `PRECIP_15`
- full month via `startdate` and `enddate`
- handle pagination using `offset` + `limit`

```python
import requests
from typing import Any, Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential

class NOAAClient:
    def __init__(self, base_url: str, token: str, timeout_s: int = 30):
        self.base_url = base_url
        self.token = token
        self.timeout_s = timeout_s

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=10))
    def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        headers = {"token": self.token}
        r = requests.get(self.base_url, headers=headers, params=params, timeout=self.timeout_s)
        r.raise_for_status()
        return r.json()

    def fetch_month(self, dataset_id: str, start_date: str, end_date: str, limit: int = 1000) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        offset = 1  # NOAA uses 1-based offsets

        while True:
            params = {
                "datasetid": dataset_id,
                "startdate": start_date,
                "enddate": end_date,
                "limit": limit,
                "offset": offset,
            }
            payload = self._get(params)
            results = payload.get("results", []) or []
            out.extend(results)

            if len(results) < limit:
                break
            offset += limit

        return out
```

---

## 7) Normalization (src/ingestion/normalizer.py)

Create `src/ingestion/normalizer.py`

### Required normalization #1: Expand concatenated strings → arrays
Common NOAA fields that may contain concatenated flags or attributes: `attributes`.
Convert:
- `"A,B,C"` → `["A","B","C"]`
- empty / null → `[]`

### Required normalization #2: Convert timestamps → epoch ms
NOAA often returns `date` like `2010-01-01T00:00:00` (ISO).
Convert to **ms**.

### Required normalization #3: Add ingestion timestamp column
Set `ingestion_ts` to “now” in epoch ms.

```python
from typing import Any, Dict, List, Optional
from dateutil import parser as dt_parser
import time

def now_ms() -> int:
    return int(time.time() * 1000)

class DataNormalizer:
    def __init__(self):
        pass

    def _split_to_array(self, s: Optional[str]) -> List[str]:
        if not s:
            return []
        # try comma first; if no comma, return single element
        parts = [p.strip() for p in s.split(",")]
        return [p for p in parts if p]

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        # Keep original
        source_record = record.copy()

        # 1) attributes string -> array
        attributes_arr = self._split_to_array(record.get("attributes"))

        # 2) date -> epoch ms
        date_str = record.get("date")
        event_time_ms = None
        if date_str:
            event_time_ms = int(dt_parser.isoparse(date_str).timestamp() * 1000)

        # 3) ingestion timestamp
        ingestion_ts = now_ms()

        return {
            "station_id": record.get("station"),
            "datatype": record.get("datatype"),
            "value": record.get("value"),
            "event_time_ms": event_time_ms,
            "attributes": attributes_arr,
            "source_record": source_record,
            "ingestion_ts": ingestion_ts,
        }
```

---

## 8) Strict Validation (Pydantic)

### 8.1 Define the schema (src/ingestion/schema.py)
Create a **strict** schema representing what you expect from NOAA + normalized fields.

```python
from pydantic import BaseModel, Field, ConfigDict
from typing import Any, Dict, List, Optional

class NormalizedPrecipRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    station_id: str = Field(min_length=1)
    datatype: str = Field(min_length=1)
    value: int
    event_time_ms: int
    attributes: List[str] = Field(default_factory=list)
    source_record: Dict[str, Any]
    ingestion_ts: int
```

### 8.2 Validator wrapper (src/ingestion/validator.py)
```python
from typing import Any, Dict, Tuple
from .schema import NormalizedPrecipRecord
from pydantic import ValidationError

class DataValidator:
    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            NormalizedPrecipRecord(**record)
            return True, ""
        except ValidationError as e:
            return False, str(e)
```

---

## 9) Ingestion Job (src/ingestion/ingest_job.py)

Ingestion flow:
1) Fetch month from NOAA
2) Normalize each record
3) Validate strictly
4) Insert valid records into Iceberg table via Trino `INSERT`

Create `src/ingestion/ingest_job.py`:

```python
import json
from typing import Any, Dict, List
from .noaa_client import NOAAClient
from .normalizer import DataNormalizer
from .validator import DataValidator
from ..db.trino_client import TrinoClient

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

        good: List[Dict[str, Any]] = []
        bad_count = 0

        for r in records:
            n = self.normalizer.normalize(r)
            ok, err = self.validator.validate(n)
            if ok:
                good.append(n)
            else:
                bad_count += 1
                # In production you'd log err + record id
                # print(f"Invalid record: {err}")

        if not good:
            print("No valid records to ingest.")
            return

        # Insert in batches to avoid huge SQL statements
        batch_size = 500
        for i in range(0, len(good), batch_size):
            batch = good[i:i+batch_size]
            self._insert_batch(batch)

        print(f"Ingested={len(good)} invalid={bad_count}")

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

            # source_record JSON (store as JSON string; Trino can CAST to JSON)
            src_json = self._sql_str(json.dumps(r["source_record"], ensure_ascii=False))

            values_sql.append(f"({station},{datatype},{value},{event_ms},{attrs_sql},CAST({src_json} AS JSON),{ingestion_ts})")

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
```

---

## 10) Incremental Transformation (Missing % per Station)

### 10.1 Watermark helper (src/transformation/watermark.py)

We store one row per job name (`station_metrics`) that tells us the latest processed `ingestion_ts`.

```python
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
```

### 10.2 Metrics Job (src/transformation/station_missing_metrics_job.py)

Rules:
- Missing/invalid is `value = 99999`
- Compute for **new data only** (based on ingestion_ts > watermark)
- Update metrics table (aggregate merge)

Simplest approach:
1) aggregate new data per station into a temp query
2) merge into the metrics table by recomputing totals as old+new

```python
from ..db.trino_client import TrinoClient
from .watermark import WatermarkStore

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
            print("No new data to transform.")
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
            print("No aggregatable new rows.")
            return

        # Merge strategy (SQL-only):
        # - Create a staging table from the aggregation
        # - Delete matching stations in metrics
        # - Insert updated totals as (old + new)
        self._merge(new_rows)

        self.watermark.upsert(self.JOB_NAME, max_ts)
        print(f"Transformed stations={len(new_rows)} watermark={max_ts}")

    def _merge(self, new_rows: list[tuple]) -> None:
        # Create staging using VALUES
        values = ",".join(f"('{sid}', {total}, {missing})" for sid, total, missing in new_rows)

        self.trino.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.station_metrics_staging (
            station_id VARCHAR,
            total_new BIGINT,
            missing_new BIGINT
        ) WITH (format='PARQUET')
        """)

        self.trino.execute(f"""DELETE FROM {self.schema}.station_metrics_staging""")
        self.trino.execute(f"""INSERT INTO {self.schema}.station_metrics_staging (station_id,total_new,missing_new) VALUES {values}""")

        # Rebuild metrics for affected stations
        self.trino.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.station_metrics_updated AS
        SELECT 1 AS dummy
        """)
        self.trino.execute(f"""DROP TABLE IF EXISTS {self.schema}.station_metrics_updated""")

        # Build updated metrics by joining old metrics + new aggregation
        self.trino.execute(f"""
        CREATE TABLE {self.schema}.station_metrics_updated AS
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
```

> Notes:
> - This SQL-only merge is straightforward and acceptable for the assignment.
> - In production you might use Iceberg MERGE (if available) or write using Spark/Flink.

---

## 11) Maintenance Pipeline (Iceberg)

Iceberg maintenance in Trino typically uses stored procedures. The exact procedure names depend on Trino + Iceberg versions, but common ones include:
- `system.expire_snapshots`
- `system.remove_orphan_files`
- `system.rewrite_data_files`

Create `src/maintenance/maintenance_job.py`:

```python
from ..db.trino_client import TrinoClient

class IcebergMaintenanceJob:
    def __init__(self, trino: TrinoClient, schema: str, raw_table: str):
        self.trino = trino
        self.schema = schema
        self.raw_table = raw_table

    def run(self) -> None:
        fqtn = f"{self.schema}.{self.raw_table}"

        # 1) Expire snapshots (keep recent ones)
        self.trino.execute(f"""
        CALL system.expire_snapshots('{fqtn}', retention_threshold => INTERVAL '1' DAY)
        """)

        # 2) Remove orphan files
        self.trino.execute(f"""
        CALL system.remove_orphan_files('{fqtn}', retention_threshold => INTERVAL '1' DAY)
        """)

        # 3) Optional: rewrite data files (compaction)
        # If not supported in your Trino version, comment out.
        self.trino.execute(f"""
        CALL system.rewrite_data_files('{fqtn}')
        """)

        print("Maintenance completed.")
```

> If a CALL is not supported by your Trino version, keep the code but:
> - catch exceptions and log “not supported”
> - OR comment out the unsupported call
> The key is to show you understand the maintenance responsibilities.

---

## 12) main.py Orchestration (src/main.py)

Replace `src/main.py` with:

```python
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
```

---

## 13) Dockerfile (One File)

Create `Dockerfile` in repo root:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src

ENV PYTHONPATH=/app

CMD ["python", "-m", "src.main"]
```

Run pipeline container locally (example):
```bash
docker build -t home-assignment-pipeline .

docker run --rm   -e NOAA_API_TOKEN="$NOAA_API_TOKEN"   -e TRINO_HOST="host.docker.internal"   -e START_DATE="2010-01-01"   -e END_DATE="2010-01-31"   home-assignment-pipeline
```

> If you run on Linux, `host.docker.internal` may not work by default; you can use:
> - `--network host` (simplest) OR
> - pass the docker bridge IP of your host

---

## 14) SQL Sanity Checks (Deliverables)

Create `sql/sanity_ingestion.sql`:

```sql
-- 1) Basic count
SELECT COUNT(*) AS row_count
FROM iceberg.home_assignment.precipitation_raw;

-- 2) Ensure required fields are present
SELECT
  SUM(CASE WHEN station_id IS NULL OR station_id = '' THEN 1 ELSE 0 END) AS missing_station,
  SUM(CASE WHEN datatype IS NULL OR datatype = '' THEN 1 ELSE 0 END) AS missing_datatype,
  SUM(CASE WHEN event_time_ms IS NULL THEN 1 ELSE 0 END) AS missing_event_time,
  SUM(CASE WHEN ingestion_ts IS NULL THEN 1 ELSE 0 END) AS missing_ingestion_ts
FROM iceberg.home_assignment.precipitation_raw;

-- 3) Date range sanity (event time)
SELECT
  from_unixtime(MIN(event_time_ms) / 1000) AS min_event_time,
  from_unixtime(MAX(event_time_ms) / 1000) AS max_event_time
FROM iceberg.home_assignment.precipitation_raw;

-- 4) Attributes normalization check: should be array
SELECT station_id, cardinality(attributes) AS attr_len
FROM iceberg.home_assignment.precipitation_raw
WHERE cardinality(attributes) > 0
LIMIT 20;

-- 5) Missing sentinel distribution
SELECT
  SUM(CASE WHEN value = 99999 THEN 1 ELSE 0 END) AS missing_count,
  COUNT(*) AS total_count,
  (SUM(CASE WHEN value = 99999 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS missing_pct
FROM iceberg.home_assignment.precipitation_raw;
```

Create `sql/sanity_transformation.sql`:

```sql
-- 1) View top stations by missing percentage
SELECT station_id, missing_percentage, total_observations, missing_observations
FROM iceberg.home_assignment.station_missing_metrics
ORDER BY missing_percentage DESC
LIMIT 50;

-- 2) Validate missing_percentage bounds
SELECT COUNT(*) AS out_of_bounds
FROM iceberg.home_assignment.station_missing_metrics
WHERE missing_percentage < 0 OR missing_percentage > 100;

-- 3) Validate missing <= total
SELECT COUNT(*) AS invalid_rows
FROM iceberg.home_assignment.station_missing_metrics
WHERE missing_observations > total_observations;

-- 4) Spot-check recomputation for one station (replace station id)
-- Compare metrics table to raw aggregation
WITH raw AS (
  SELECT
    station_id,
    COUNT(*) AS total_raw,
    SUM(CASE WHEN value = 99999 THEN 1 ELSE 0 END) AS missing_raw
  FROM iceberg.home_assignment.precipitation_raw
  WHERE station_id = 'REPLACE_ME'
  GROUP BY 1
),
met AS (
  SELECT station_id, total_observations, missing_observations
  FROM iceberg.home_assignment.station_missing_metrics
  WHERE station_id = 'REPLACE_ME'
)
SELECT
  raw.station_id,
  raw.total_raw,
  raw.missing_raw,
  met.total_observations,
  met.missing_observations
FROM raw
LEFT JOIN met ON raw.station_id = met.station_id;
```

---

## 15) Architecture Diagram (ARCHITECTURE.md)

Create `ARCHITECTURE.md`:

```md
# Architecture

NOAA API (PRECIP_15)
  |
  |  (HTTP, paginated fetch)
  v
Python Ingestion (normalize + validate)
  |
  |  (Trino INSERT into Iceberg)
  v
Iceberg Table on MinIO (S3 warehouse)
  |
  |  (Incremental SQL aggregation using ingestion_ts watermark)
  v
Metrics Iceberg Table (missing % per station)
  |
  |  (Iceberg maintenance procedures: expire snapshots, remove orphan files, rewrite data files)
  v
Optimized Iceberg Storage + Clean Metadata
```

Add production-grade notes (brief):
- retries + backoff for API
- idempotency (ingestion_ts + dedupe if needed)
- partitioning strategy (by station/date)
- observability (logs/metrics)

---

## 16) Cursor “What to Do Next” Checklist

Use this checklist as Cursor tasks (in order):

1) Create the folder structure and files exactly as in section 2.1
2) Implement `Settings` in `src/config.py`
3) Implement `TrinoClient` in `src/db/trino_client.py`
4) Implement `ddl.py` with schema + three tables
5) Implement `NOAAClient` with pagination + retry
6) Implement `DataNormalizer` (3 required normalizations)
7) Implement strict Pydantic schema + validator
8) Implement ingestion job: fetch → normalize → validate → batch insert
9) Implement watermark store
10) Implement incremental transformation job (new data only)
11) Implement maintenance job with Iceberg procedures (handle unsupported CALLs gracefully)
12) Wire everything in `src/main.py`
13) Add Dockerfile and verify container can run pipeline
14) Run sanity SQLs and copy results into README as proof

---

## 17) Common Pitfalls (Avoid)

- Forgetting that NOAA pagination uses `offset` and `limit`
- Not converting timestamps to **epoch ms**
- Storing attributes as string instead of `ARRAY(VARCHAR)`
- Recomputing transformation on all history (must be incremental)
- Not providing SQL sanity checks (required)

---

## 18) Done Criteria

You are done when:
- `docker compose up -d` starts the datalake
- `docker run ... home-assignment-pipeline` ingests and transforms successfully
- Trino queries show:
  - raw rows exist
  - metrics exist
  - missing_percentage is sensible and bounded
- Maintenance step runs (or logs that a procedure is not supported) and completes
- Repo contains: code, Dockerfile, architecture diagram, SQL files, README instructions
