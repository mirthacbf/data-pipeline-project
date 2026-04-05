import duckdb

# conexión a MotherDuck
con = duckdb.connect("md:myproyecto")

# conectar postgres
con.execute("""
ATTACH 'postgresql://dbt:dbt@localhost:5432/analytics' AS pg
""")

# exportar tabla
con.execute("""
CREATE OR REPLACE TABLE pg.fact_sales AS
SELECT * FROM fact_sales
""")

print("✅ Sync completado")