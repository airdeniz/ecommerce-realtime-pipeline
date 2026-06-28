-- Mantik tutarliligi:
--   iptal sayisi <= toplam siparis
--   tekil musteri sayisi <= toplam siparis
-- Bu kosullari ihlal eden bir gun donerse test BASARISIZ olur.
SELECT
    order_date,
    total_orders,
    cancelled_orders,
    unique_customers
FROM {{ ref('mart_daily_revenue') }}
WHERE cancelled_orders > total_orders
   OR unique_customers > total_orders
