WITH source AS (
    SELECT
        op,
        lsn,
        ts_ms,
        user_id,
        get_json_object(raw_payload, '$.full_name')             AS full_name,
        get_json_object(raw_payload, '$.city')                  AS city,
        CAST(get_json_object(raw_payload, '$.created_at') AS TIMESTAMP) AS created_at
    FROM {{ source('bronze', 'users') }}
    WHERE op IN ('c', 'u', 'r', 'd')
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
