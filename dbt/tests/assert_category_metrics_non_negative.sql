-- Kategori bazli metrikler negatif olamaz.
-- Bir satir donerse test BASARISIZ olur.
SELECT
    category,
    total_quantity,
    total_revenue
FROM {{ ref('mart_sales_by_category') }}
WHERE total_quantity < 0
   OR total_revenue < 0
