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
    -- r = ilk snapshot (mevcut kayitlar), c = create, u = update, d = delete.
    -- Hepsini aliyoruz: snapshot baslangic verisidir, delete ise asagida
    -- is_deleted'a cevrilir (hard delete yerine soft delete).
    WHERE op IN ('c', 'u', 'r', 'd')
),

-- Bir order_id icin birden fazla event (CREATED, PAID, hatta DELETE) ayni
-- created_at'e sahip olabildiginden, en guncel versiyonu Debezium WAL LSN'ine
-- gore seciyoruz. Eger en guncel event delete ise o kazanir ve is_deleted=true olur.
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
    created_at,
    CASE WHEN op = 'd' THEN TRUE ELSE FALSE END AS is_deleted
FROM deduped
WHERE rn = 1