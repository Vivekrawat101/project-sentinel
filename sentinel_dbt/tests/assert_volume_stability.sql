-- tests/assert_volume_stability.sql

WITH recent_data AS (
    -- 1. Grab the most recent ingestion day from your new baseline table
    SELECT 
        ingestion_date,
        daily_order_count,
        moving_average_7d
    FROM {{ ref('fct_daily_volume') }}
    ORDER BY ingestion_date DESC
    LIMIT 1
)

-- 2.Failsafe Condition
-- If this query returns a row, it means the volume was anomalous, and dbt will FAIL the test.
SELECT *
FROM recent_data
WHERE 
    -- Condition A: Volume dropped by more than 50%
    daily_order_count < (moving_average_7d * 0.5)
    OR
    -- Condition B: Volume spiked by more than 200%
    daily_order_count > (moving_average_7d * 2.0)