-- Ciro hicbir gun negatif olmamali.
-- Bir satir donerse test BASARISIZ olur.
SELECT
    order_date,
    total_revenue
FROM {{ ref('mart_daily_revenue') }}
WHERE total_revenue < 0
