SELECT
    DATE_TRUNC('month', sale_date) AS mes,
    SUM(revenue) AS total_ventas
FROM fact_sales
GROUP BY 1
ORDER BY 1;