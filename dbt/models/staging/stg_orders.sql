WITH source AS (
    SELECT
        op,
        order_id,
        user_id,
        status,
        CAST(total_amount AS DECIMAL(12,2)) AS total_amount,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM lakehouse.bronze.orders
    WHERE op IN ('c', 'u')
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY created_at DESC
        ) AS rn
    FROM source
)

SELECT
    order_id,
    user_id,
    status,
    total_amount,
    created_at
FROM deduped
WHERE rn = 1