{{ config(
    materialized='table'
) }}

WITH daily_counts AS (
    -- 1. Aggregate the raw JSON payloads into a daily count
    SELECT 
        DATE(CAST(order_timestamp AS TIMESTAMP)) AS ingestion_date,
        COUNT(order_id) AS daily_order_count
    FROM {{ source('raw', 'daily_orders') }}
    GROUP BY 1
),

moving_average_calc AS (
    -- 2. Apply the Window Function to calculate the 7-day trend
    SELECT 
        ingestion_date,
        daily_order_count,
        AVG(daily_order_count) OVER (
            ORDER BY ingestion_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_average_7d
    FROM daily_counts
)

-- 3. Final output rounded for clean comparison
SELECT 
    ingestion_date,
    daily_order_count,
    ROUND(moving_average_7d, 2) AS moving_average_7d
FROM moving_average_calc