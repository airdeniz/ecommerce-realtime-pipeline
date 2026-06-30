-- Order-level feature table for the anomaly / fraud model (IsolationForest).
-- One row per non-deleted order, with intrinsic order characteristics only
-- (amount, basket shape, per-item prices, hour of day). The model is
-- unsupervised, so no label is produced here.

WITH items AS (
    SELECT
        order_id,
        COUNT(*)                  AS item_count,
        COUNT(DISTINCT product_id) AS distinct_products,
        SUM(quantity)             AS total_quantity,
        AVG(unit_price)           AS avg_unit_price,
        MAX(unit_price)           AS max_unit_price
    FROM {{ ref('core_order_items') }}
    WHERE is_deleted = FALSE
    GROUP BY order_id
),

orders AS (
    SELECT
        order_id,
        user_id,
        status,
        total_amount,
        created_at
    FROM {{ ref('core_orders') }}
    WHERE is_deleted = FALSE
)

SELECT
    o.order_id,
    o.user_id,
    o.status,
    o.total_amount,
    COALESCE(i.item_count, 0)        AS item_count,
    COALESCE(i.distinct_products, 0) AS distinct_products,
    COALESCE(i.total_quantity, 0)    AS total_quantity,
    COALESCE(i.avg_unit_price, 0)    AS avg_unit_price,
    COALESCE(i.max_unit_price, 0)    AS max_unit_price,
    HOUR(o.created_at)               AS hour_of_day,
    o.created_at
FROM orders o
LEFT JOIN items i ON o.order_id = i.order_id
