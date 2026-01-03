# Run Instructions

## Prerequisites

1. **Docker and Docker Compose** installed
2. **NOAA API Token** - Get one from: https://www.ncdc.noaa.gov/cdo-web/webservices/v2#gettingStarted
3. **Python 3.11+** (if running locally without Docker)

## Step 1: Start the Datalake (Trino + Iceberg REST + MinIO)

From the repository root:

```bash
cd datalake/trino
docker compose up -d
```

Wait for services to start (about 30-60 seconds), then verify:

- **Trino UI**: http://localhost:8080
- **Iceberg REST**: http://localhost:8183
- **MinIO Console**: http://localhost:9000 (default credentials: minioadmin/minioadmin)

## Step 2: Set Environment Variables

Export your NOAA API token and optionally customize other settings:

```bash
export NOAA_API_TOKEN="your_noaa_token_here"
export START_DATE="2010-01-01"
export END_DATE="2010-01-31"
export TRINO_HOST="localhost"
export TRINO_PORT="8080"
export TRINO_USER="pipeline"
export TRINO_CATALOG="iceberg"
export TRINO_SCHEMA="home_assignment"
```

## Step 3: Run the Pipeline

### Option A: Run Locally (Python)

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the pipeline:
```bash
python -m src.main
```

### Option B: Run with Docker (Recommended)

1. Build the Docker image:
```bash
docker build -t home-assignment-pipeline .
```

2. Run the container:
```bash
docker run --rm \
  -e NOAA_API_TOKEN="$NOAA_API_TOKEN" \
  -e START_DATE="2010-01-01" \
  -e END_DATE="2010-01-31" \
  -e TRINO_HOST="host.docker.internal" \
  -e TRINO_PORT="8080" \
  -e TRINO_USER="pipeline" \
  -e TRINO_CATALOG="iceberg" \
  -e TRINO_SCHEMA="home_assignment" \
  home-assignment-pipeline
```

**Note for Linux users**: `host.docker.internal` may not work. Use one of:
- `--network host` flag instead
- Or find your Docker bridge IP and use that

## Step 4: Verify Results

### Check Trino UI

1. Open http://localhost:8080
2. Run queries to verify data:

```sql
-- Check raw table
SELECT COUNT(*) FROM iceberg.home_assignment.precipitation_raw;

-- Check metrics table
SELECT COUNT(*) FROM iceberg.home_assignment.station_missing_metrics;

-- View sample data
SELECT * FROM iceberg.home_assignment.precipitation_raw LIMIT 10;
```

### Run SQL Sanity Checks

Execute the provided SQL files in Trino UI or via CLI:

```bash
# Using Trino CLI (if installed)
trino --server localhost:8080 --execute "$(cat sql/sanity_ingestion.sql)"
trino --server localhost:8080 --execute "$(cat sql/sanity_transformation.sql)"
```

Or copy-paste queries from:
- `sql/sanity_ingestion.sql` - Validates ingestion correctness
- `sql/sanity_transformation.sql` - Validates transformation results

## Pipeline Flow

The pipeline executes in this order:

1. **Init**: Creates schema and tables (raw, metrics, watermark)
2. **Ingest**: Fetches NOAA data → normalizes → validates → inserts into Iceberg
3. **Transform**: Computes missing % per station (incremental, only new data)
4. **Maintain**: Runs Iceberg maintenance (expire snapshots, remove orphans, optimize)

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `NOAA_API_TOKEN` | (required) | Your NOAA API token |
| `START_DATE` | `2010-01-01` | Start date for data ingestion |
| `END_DATE` | `2010-01-31` | End date for data ingestion |
| `TRINO_HOST` | `localhost` | Trino server hostname |
| `TRINO_PORT` | `8080` | Trino server port |
| `TRINO_USER` | `pipeline` | Trino username |
| `TRINO_CATALOG` | `iceberg` | Trino catalog name |
| `TRINO_SCHEMA` | `home_assignment` | Trino schema name |
| `RAW_TABLE` | `precipitation_raw` | Raw data table name |
| `METRICS_TABLE` | `station_missing_metrics` | Metrics table name |
| `WATERMARK_TABLE` | `pipeline_watermark` | Watermark table name |
| `NOAA_PAGE_LIMIT` | `1000` | NOAA API pagination limit |
| `REQUEST_TIMEOUT_S` | `30` | HTTP request timeout in seconds |

## Troubleshooting

### Datalake services not starting
- Check if ports 8080, 8183, 9000 are available
- Review logs: `docker compose logs` in `datalake/trino/`

### Connection errors to Trino
- Ensure datalake is running: `docker compose ps` in `datalake/trino/`
- For Docker container, use `host.docker.internal` (macOS/Windows) or `--network host` (Linux)

### NOAA API errors
- Verify your token is valid
- Check rate limits (NOAA has request limits)
- Ensure date range has data available (try 2010-01-01 to 2010-01-31)

### Validation errors
- Check logs for specific validation failures
- Invalid records are skipped (counted in output)

## Stopping Services

To stop the datalake:

```bash
cd datalake/trino
docker compose down
```

To stop and remove volumes (clean slate):

```bash
cd datalake/trino
docker compose down -v
```

