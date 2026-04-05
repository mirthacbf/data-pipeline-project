SELECT
    ship_mode,
    COUNT(*) AS total
FROM fact_sales
GROUP BY ship_mode;