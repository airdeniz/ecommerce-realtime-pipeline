WITH source AS (
    SELECT
        op,
        lsn,
        ts_ms,
        order_id,
        -- Is kolonlari bronze'da ayri kolon degil; raw_payload JSON'undan
        -- cikariliyor. Kaynaga yeni kolon eklenirse buraya bir satir eklemek
        -- yeterli; bronze semasi degismez, gecmis veri raw_payload'da hazir.
        get_json_object(raw_payload, '$.user_id')      AS user_id,
        get_json_object(raw_payload, '$.status')       AS status,
        CAST(get_json_object(raw_payload, '$.total_amount') AS DECIMAL(12,2)) AS total_amount,
        CAST(get_json_object(raw_payload, '$.created_at') AS TIMESTAMP)       AS created_at
    FROM {{ source('bronze', 'orders') }}
    -- r = ilk snapshot, c = create, u = update, d = delete. Hepsini aliyoruz;
    -- delete asagida is_deleted'a cevrilir (soft delete).
    WHERE op IN ('c', 'u', 'r', 'd')
),

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
    CAST(user_id AS BIGINT) AS user_id,
    status,
    total_amount,
    created_at,
    CASE WHEN op = 'd' THEN TRUE ELSE FALSE END AS is_deleted
FROM deduped
WHERE rn = 1
