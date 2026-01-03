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

