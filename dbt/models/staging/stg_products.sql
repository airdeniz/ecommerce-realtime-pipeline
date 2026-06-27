WITH source AS (
    SELECT
        op,
        product_id,
        name,
        category,
        CAST(price AS DECIMAL(10,2)) AS price
    FROM lakehouse.bronze.products
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_id
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