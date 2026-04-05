SELECT
    product_id,
    SUM(quantity) AS total
FROM fact_sales
GROUP BY product_id
ORDER BY total DESC;