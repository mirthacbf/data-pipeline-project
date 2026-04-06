# Data Pipeline End-to-End

## Arquitectura
Airbyte → MotherDuck → dbt → Prefect → Metabase


## Cómo ejecutar

1. Levantar servicios:
    * Docker
    * Airbyte
    * MySQL
    * MotherDuck
    * Metabase

2. Ejecutar pipeline:
python prefect/flow_pipeline.py

3. Ejecutar dbt manual:
cd dbt
dbt run
dbt test

## Modelado
Se utilizó modelo dimensional (Kimball)
Se eligió este modelo para estructurar los datos en hechos y dimensiones, optimizando consultas analíticas y facilitando la creación de dashboards en Metabase.

## Calidad
Se implementaron tests con dbt-expectations

## Dashboard
Disponible en Metabase

El .env  (cargado con credenciales) dbt NO lo lee automáticamente
Carga el token en la terminal de VS (Bash)

export MOTHERDUCK_TOKEN=eyJhbGciOiJIUzI1NiIs...

Para verificar si funciona  

echo $MOTHERDUCK_TOKEN

En Bash se carga automáticamente con

export $(grep -v '^#' .env | xargs)

Otra forma de cargar
# 1. activar entorno
source venv/Scripts/activate   # o según tu entorno

# 2. cargar variables
set -a
source .env
set +a

# 3. probar
dbt debug

# 4. ejecutar
dbt run

Agregar filtros:
    fecha
    categoría
    estado
