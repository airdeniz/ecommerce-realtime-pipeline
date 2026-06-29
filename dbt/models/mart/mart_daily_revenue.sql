WITH orders AS (
    SELECT * FROM {{ ref('core_orders') }}
),

daily AS (
    SELECT
        DATE(created_at) AS order_date,
        COUNT(order_id) AS total_orders,
        SUM(paid_amount) AS total_revenue,
        SUM(is_cancelled) AS cancelled_orders,
        COUNT(DISTINCT user_id) AS unique_customers
    FROM orders
    -- Silinen siparisler (soft delete) metriklere dahil edilmez.
    WHERE is_deleted = FALSE
    GROUP BY DATE(created_at)
)

SELECT * FROM daily
ORDER BY order_date DESC