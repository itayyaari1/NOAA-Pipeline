# Architecture

## Data Flow

```
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

## Components

### 1. Data Source
- **NOAA API**: REST API providing PRECIP_15 dataset
- Pagination: 1000 records per page
- Rate limiting: Handled with retry logic

### 2. Ingestion Layer
- **NOAA Client**: Fetches data with pagination and retry (exponential backoff)
- **Normalizer**: Applies 3 required transformations:
  1. Expand concatenated strings → arrays
  2. Convert timestamps → epoch ms
  3. Add ingestion timestamp
- **Validator**: Strict Pydantic validation
- **Ingest Job**: Orchestrates fetch → normalize → validate → batch insert

### 3. Storage Layer
- **Trino**: SQL query engine
- **Iceberg REST Catalog**: Metadata management
- **MinIO**: S3-compatible object storage (warehouse)
- **PostgreSQL**: Catalog metadata storage

### 4. Transformation Layer
- **Watermark Store**: Tracks last processed ingestion_ts per job
- **Metrics Job**: Incremental aggregation (only new data)
- Computes missing percentage per station (value = 99999)

### 5. Maintenance Layer
- **Maintenance Job**: Iceberg table optimization
  - Expire old snapshots
  - Remove orphan files
  - Rewrite data files (compaction)

## Production-Grade Considerations

### Retries + Backoff
- **NOAA API**: Implemented with `tenacity` library
  - 5 retry attempts
  - Exponential backoff (1-10 seconds)
  - Handles transient network errors

### Idempotency
- **ingestion_ts**: Unique timestamp per record ensures no duplicate processing
- **Watermark**: Tracks processed data to prevent reprocessing
- **Deduplication**: Can be added by checking ingestion_ts before insert

### Partitioning Strategy
- **Current**: No explicit partitioning (Iceberg handles file-level organization)
- **Recommended for production**: Partition by:
  - Date (year/month) for time-based queries
  - Station ID for station-specific queries
  - Example: `PARTITIONED BY (year(event_time_ms), month(event_time_ms))`

### Observability
- **Logging**: Centralized in `src/utils/logging.py`
  - Step-by-step progress tracking
  - Batch/page progress for long operations
  - Error handling with stack traces
- **Metrics**: Can be extended with:
  - Record counts per batch
  - Processing time per step
  - API call metrics
  - Validation failure rates

### Scalability
- **Batch Processing**: 500 records per batch for optimal performance
- **Incremental Processing**: Only processes new data (watermark-based)
- **Parallel Processing**: Can be extended to process multiple stations in parallel

### Error Handling
- **Validation Failures**: Logged and skipped (counted in output)
- **API Failures**: Retried with exponential backoff
- **Trino Failures**: Propagated with error messages
- **Maintenance Procedures**: Gracefully handles unsupported operations

## Technology Stack

- **Python 3.11+**: Pipeline language
- **Trino 478**: SQL query engine
- **Apache Iceberg**: Table format
- **MinIO**: S3-compatible storage
- **PostgreSQL**: Catalog metadata
- **Pydantic**: Data validation
- **Tenacity**: Retry logic

