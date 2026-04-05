SELECT
    postal_code,
    COUNT(*) AS total_sales
FROM fact_sales
GROUP BY postal_code
ORDER BY total_sales DESC
LIMIT 10;