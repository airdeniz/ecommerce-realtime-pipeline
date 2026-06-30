-- Hourly revenue/order time series for the demand-forecast model (Prophet).
-- Hourly grain is used because the synthetic data accumulates over hours, so an
-- hourly series has enough points to forecast within a single day of runtime;
-- the daily-grain forecast reuses the existing gold.mart_daily_revenue table.

WITH orders AS (
    SELECT
        order_id,
        paid_amount,
        created_at
    FROM {{ ref('core_orders') }}
    WHERE is_deleted = FALSE
)

SELECT
    DATE_TRUNC('HOUR', created_at) AS revenue_hour,
    COUNT(order_id)                AS total_orders,
    SUM(paid_amount)               AS total_revenue
FROM orders
GROUP BY DATE_TRUNC('HOUR', created_at)
ORDER BY revenue_hour
