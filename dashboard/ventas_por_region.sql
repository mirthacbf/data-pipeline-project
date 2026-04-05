SELECT
    region,
    COUNT(*) AS total_sales
FROM fact_sales
GROUP BY region
ORDER BY total_sales DESC;