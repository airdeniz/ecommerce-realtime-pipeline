WITH source AS (
    SELECT
        op,
        lsn,
        ts_ms,
        order_id,
        user_id,
        status,
        CAST(total_amount AS DECIMAL(12,2)) AS total_amount,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM {{ source('bronze', 'orders') }}
    WHERE op IN ('c', 'u')
),

-- Bir order_id icin CREATED ve PAID/CANCELLED satirlari ayni created_at'e
-- sahip oldugundan, en guncel versiyonu Debezium WAL LSN'ine gore seciyoruz.
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY lsn DESC, ts_ms DESC
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