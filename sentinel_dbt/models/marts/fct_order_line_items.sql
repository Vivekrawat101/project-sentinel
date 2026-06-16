{{ config(materialized='table') }}

WITH snapshot_history AS (
    SELECT 
        line_item_id,
        
        -- Current vs Previous Status
        item_status AS current_status,
        LAG(item_status) OVER (PARTITION BY line_item_id ORDER BY dbt_valid_from ASC) AS previous_status,
        
        -- Current vs Previous Carrier
        shipping_carrier AS current_carrier,
        LAG(shipping_carrier) OVER (PARTITION BY line_item_id ORDER BY dbt_valid_from ASC) AS previous_carrier,

        -- Current vs Previous Fulfillment Center
        fulfillment_center AS current_fulfillment_center,
        LAG(fulfillment_center) OVER (PARTITION BY line_item_id ORDER BY dbt_valid_from ASC) AS previous_fulfillment_center,

        -- Current vs Previous Return Reason
        return_reason AS current_return_reason,
        LAG(return_reason) OVER (PARTITION BY line_item_id ORDER BY dbt_valid_from ASC) AS previous_return_reason,
        
        -- Time tracking
        dbt_valid_from AS state_started_at,
        dbt_valid_to AS state_ended_at,
        
        -- Flag to instantly find the currently active rows
        CASE 
            WHEN dbt_valid_to IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current_state

    FROM {{ ref('line_items_snapshot') }}
)

SELECT 
    line_item_id,
    current_status,
    previous_status,
    current_carrier,
    previous_carrier,
    current_fulfillment_center,
    previous_fulfillment_center,
    current_return_reason,
    previous_return_reason,
    state_started_at,
    state_ended_at,
    is_current_state,
    
    -- Analytics Flags for PowerBI

    CASE WHEN current_status IS DISTINCT FROM previous_status THEN TRUE ELSE FALSE END AS is_status_change_event,
    CASE WHEN current_carrier IS DISTINCT FROM previous_carrier THEN TRUE ELSE FALSE END AS is_carrier_change_event,
    CASE WHEN current_fulfillment_center IS DISTINCT FROM previous_fulfillment_center THEN TRUE ELSE FALSE END AS is_fulfillment_change_event,
    CASE WHEN current_return_reason IS DISTINCT FROM previous_return_reason THEN TRUE ELSE FALSE END AS is_return_change_event

FROM snapshot_history