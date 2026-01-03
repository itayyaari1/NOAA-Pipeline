def create_schema(schema: str) -> str:
    return f"""CREATE SCHEMA IF NOT EXISTS {schema}"""

def create_raw_table(schema: str, table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        station_id VARCHAR,
        datatype VARCHAR,
        value BIGINT,
        event_time_ms BIGINT,
        attributes ARRAY(VARCHAR),
        source_record VARCHAR,
        ingestion_ts BIGINT
    )
    WITH (
        format = 'PARQUET',
        location = 's3://iceberg/{schema}/{table}'
    )
    """

def create_metrics_table(schema: str, table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        station_id VARCHAR,
        total_observations BIGINT,
        missing_observations BIGINT,
        missing_percentage DOUBLE,
        last_updated_ts BIGINT
    )
    WITH (
        format='PARQUET',
        location = 's3://iceberg/{schema}/{table}'
    )
    """

def create_watermark_table(schema: str, table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        job_name VARCHAR,
        last_processed_ingestion_ts BIGINT,
        updated_ts BIGINT
    )
    WITH (
        format='PARQUET',
        location = 's3://iceberg/{schema}/{table}'
    )
    """

