WITH source AS (
    SELECT
        op,
        lsn,
        ts_ms,
        order_item_id,
        order_id,
        product_id,
        quantity,
        CAST(unit_price AS DECIMAL(10,2)) AS unit_price
    FROM {{ source('bronze', 'order_items') }}
    WHERE op IN ('c', 'u', 'r')
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY lsn DESC, ts_ms DESC
        ) AS rn
    FROM source
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price
FROM deduped
WHERE rn = 1