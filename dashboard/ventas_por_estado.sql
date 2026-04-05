SELECT
    state,
    SUM(revenue) AS total
FROM fact_sales
GROUP BY state
ORDER BY total DESC;