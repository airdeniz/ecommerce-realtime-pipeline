WITH source AS (
    SELECT
        op,
        lsn,
        ts_ms,
        product_id,
        name,
        category,
        CAST(price AS DECIMAL(10,2)) AS price
    FROM {{ source('bronze', 'products') }}
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
    price
FROM deduped
WHERE rn = 1