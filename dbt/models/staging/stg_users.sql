WITH source AS (
    SELECT
        op,
        user_id,
        full_name,
        city,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM lakehouse.bronze.users
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY created_at DESC
        ) AS rn
    FROM source
)

SELECT
    user_id,
    full_name,
    city,
    created_at
FROM deduped
WHERE rn = 1