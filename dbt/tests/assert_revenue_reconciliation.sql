-- Mutabakat (reconciliation):
-- mart_daily_revenue.total_revenue, core_orders icindeki paid_amount toplamina
-- gun bazinda esit olmali. Kuruş yuvarlama toleransi 0.01.
-- Fark donerse test BASARISIZ olur -> gold ile silver tutarsiz demektir.
WITH mart AS (
    SELECT
        order_date,
        total_revenue
    FROM {{ ref('mart_daily_revenue') }}
),

core AS (
    SELECT
        DATE(created_at) AS order_date,
        SUM(paid_amount) AS revenue
    FROM {{ ref('core_orders') }}
    GROUP BY DATE(created_at)
)

SELECT
    m.order_date,
    m.total_revenue,
    c.revenue,
    ABS(m.total_revenue - c.revenue) AS diff
FROM mart m
FULL OUTER JOIN core c ON m.order_date = c.order_date
WHERE ABS(COALESCE(m.total_revenue, 0) - COALESCE(c.revenue, 0)) > 0.01
