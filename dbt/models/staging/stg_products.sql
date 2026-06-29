WITH source AS (
    SELECT
        op,
        lsn,
        ts_ms,
        product_id,
        get_json_object(raw_payload, '$.name')          AS name,
        get_json_object(raw_payload, '$.category')      AS category,
        CAST(get_json_object(raw_payload, '$.price') AS DECIMAL(10,2)) AS price
    FROM {{ source('bronze', 'products') }}
    WHERE op IN ('c', 'u', 'r', 'd')
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY lsn DESC, ts_ms DESC
        ) AS rn
    FROM source
)

SELECT
    product_id,
    name,
    category,
    price,
    CASE WHEN op = 'd' THEN TRUE ELSE FALSE END AS is_deleted
FROM deduped
WHERE rn = 1
