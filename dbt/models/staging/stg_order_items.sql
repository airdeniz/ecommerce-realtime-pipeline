WITH source AS (
    SELECT
        op,
        lsn,
        ts_ms,
        order_item_id,
        CAST(get_json_object(raw_payload, '$.order_id') AS BIGINT)    AS order_id,
        CAST(get_json_object(raw_payload, '$.product_id') AS BIGINT)  AS product_id,
        CAST(get_json_object(raw_payload, '$.quantity') AS BIGINT)    AS quantity,
        CAST(get_json_object(raw_payload, '$.unit_price') AS DECIMAL(10,2)) AS unit_price
    FROM {{ source('bronze', 'order_items') }}
    WHERE op IN ('c', 'u', 'r', 'd')
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
    unit_price,
    CASE WHEN op = 'd' THEN TRUE ELSE FALSE END AS is_deleted
FROM deduped
WHERE rn = 1
