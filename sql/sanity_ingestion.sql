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

