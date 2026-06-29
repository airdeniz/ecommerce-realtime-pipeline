WITH source AS (
    SELECT
        op,
        lsn,
        ts_ms,
        user_id,
        full_name,
        city,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM {{ source('bronze', 'users') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY lsn DESC, ts_ms DESC
        ) AS rn
    FROM source
)

SELECT
    user_id,
    full_name,
    city,
    created_at,
    CASE WHEN op = 'd' THEN TRUE ELSE FALSE END AS is_deleted
FROM deduped
WHERE rn = 1