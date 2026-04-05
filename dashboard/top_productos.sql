SELECT
    category,
    SUM(revenue) AS total
FROM fact_sales
GROUP BY category
ORDER BY total DESC;