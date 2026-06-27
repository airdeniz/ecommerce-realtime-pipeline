WITH order_items AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        CAST(unit_price AS DECIMAL(10,2)) AS unit_price
    FROM lakehouse.bronze.order_items
    WHERE op IN ('c', 'u')
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

final AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        p.name AS product_name,
        p.category,
        oi.quantity,
        oi.unit_price,
        oi.quantity * oi.unit_price AS line_total
    FROM order_items oi
    LEFT JOIN products p ON oi.product_id = p.product_id
)

SELECT * FROM final