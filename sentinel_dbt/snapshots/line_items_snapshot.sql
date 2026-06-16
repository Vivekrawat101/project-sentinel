{% snapshot line_items_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='line_item_id',
      strategy='check',
      check_cols=['item_status', 'shipping_carrier', 'fulfillment_center', 'return_reason'],
      invalidate_hard_deletes=True
    )
}}

WITH shredded_payloads AS (
    SELECT 
        order_id,
        order_timestamp,
        jsonb_array_elements(line_items::jsonb) AS item_payload
    FROM {{ source('raw', 'daily_orders') }}
),
extracted AS (
    SELECT
        order_id,
        order_timestamp,
        (item_payload ->> 'line_item_id')::text AS line_item_id,
        (item_payload ->> 'item_sku')::text AS item_sku,
        (item_payload ->> 'category')::text AS category,
        (item_payload ->> 'brand')::text AS brand,
        (item_payload ->> 'supplier')::text AS supplier,
        (item_payload ->> 'unit_price')::numeric AS unit_price,
        (item_payload ->> 'quantity')::int AS quantity,
        (item_payload ->> 'fulfillment_center')::text AS fulfillment_center,
        (item_payload ->> 'shipping_carrier')::text AS shipping_carrier,
        (item_payload ->> 'item_status')::text AS item_status,
        (item_payload ->> 'promo_code_applied')::text AS promo_code_applied,
        (item_payload ->> 'return_reason')::text AS return_reason
    FROM shredded_payloads
),
deduplicated AS (
    SELECT 
        order_id,
        order_timestamp,
        line_item_id,
        item_sku,
        category,
        brand,
        supplier,
        unit_price,
        quantity,
        fulfillment_center,
        shipping_carrier,
        item_status,
        promo_code_applied,
        return_reason,
        ROW_NUMBER() OVER (PARTITION BY line_item_id ORDER BY order_timestamp DESC) as rn
    FROM extracted
)

SELECT 
    order_id,
    order_timestamp,
    line_item_id,
    item_sku,
    category,
    brand,
    supplier,
    unit_price,
    quantity,
    fulfillment_center,
    shipping_carrier,
    item_status,
    promo_code_applied,
    return_reason
FROM deduplicated
WHERE rn = 1

{% endsnapshot %}