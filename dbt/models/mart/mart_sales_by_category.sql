WITH order_items AS (
    SELECT * FROM {{ ref('core_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('core_orders') }}
),

final AS (
    SELECT
        oi.category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.quantity) AS total_quantity,
        SUM(oi.line_total) AS total_revenue
    FROM order_items oi
    LEFT JOIN orders o ON oi.order_id = o.order_id
    WHERE o.status = 'PAID'
      AND o.is_deleted = FALSE
      AND oi.is_deleted = FALSE
    GROUP BY oi.category
)

SELECT * FROM final
ORDER BY total_revenue DESC