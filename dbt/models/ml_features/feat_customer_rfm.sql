-- Customer-level RFM + behavioural feature table. One row per user. Powers both
-- the segmentation model (KMeans) and the churn model (proxy label).
--
-- Recency is measured in FRACTIONAL days relative to the dataset's latest order
-- (MAX(created_at)), not wall-clock now(): the synthetic data spans only as long
-- as the generator has been running, so an absolute "days since today" would be
-- meaningless on a fresh stack. Recency vs the latest event keeps it scale-free.

WITH orders AS (
    SELECT
        user_id,
        order_id,
        paid_amount,
        total_amount,
        is_cancelled,
        created_at
    FROM {{ ref('core_orders') }}
    WHERE is_deleted = FALSE
),

bounds AS (
    SELECT MAX(created_at) AS max_ts FROM orders
),

agg AS (
    SELECT
        user_id,
        COUNT(*)                AS frequency,
        SUM(paid_amount)        AS monetary,
        AVG(total_amount)       AS avg_basket,
        SUM(is_cancelled)       AS cancelled_orders,
        MAX(created_at)         AS last_order_ts,
        MIN(created_at)         AS first_order_ts
    FROM orders
    GROUP BY user_id
),

cats AS (
    SELECT
        o.user_id,
        COUNT(DISTINCT oi.category) AS distinct_categories
    FROM orders o
    JOIN {{ ref('core_order_items') }} oi
        ON o.order_id = oi.order_id AND oi.is_deleted = FALSE
    GROUP BY o.user_id
)

SELECT
    a.user_id,
    -- CAST the numerator to DOUBLE so the result is DOUBLE, not DECIMAL: dividing
    -- a BIGINT by the literal 86400.0 (a decimal) would otherwise yield DECIMAL,
    -- which surfaces as Python Decimal in pandas and breaks float arithmetic.
    CAST(UNIX_TIMESTAMP(b.max_ts) - UNIX_TIMESTAMP(a.last_order_ts) AS DOUBLE) / 86400.0  AS recency_days,
    a.frequency,
    a.monetary,
    a.avg_basket,
    CAST(UNIX_TIMESTAMP(b.max_ts) - UNIX_TIMESTAMP(a.first_order_ts) AS DOUBLE) / 86400.0 AS tenure_days,
    COALESCE(c.distinct_categories, 0)                                       AS distinct_categories,
    CAST(a.cancelled_orders AS DOUBLE) / a.frequency                         AS cancel_rate
FROM agg a
CROSS JOIN bounds b
LEFT JOIN cats c ON a.user_id = c.user_id
