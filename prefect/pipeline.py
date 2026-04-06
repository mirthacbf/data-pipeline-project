import os
import time
import requests
import subprocess
from prefect import flow, task
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

AIRBYTE_API_URL = os.getenv("AIRBYTE_API_URL")
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")

# -----------------------------
# TASK 1: Ejecutar Airbyte Sync
# -----------------------------
@task(retries=2, retry_delay_seconds=10)
def run_airbyte_sync():
    url = f"{AIRBYTE_API_URL}/connections/sync"

    payload = {
        "connectionId": AIRBYTE_CONNECTION_ID
    }

    print(" Iniciando sync de Airbyte...")

    response = requests.post(url, json=payload)

    if response.status_code != 200:
        raise Exception(f"Error iniciando sync: {response.text}")

    job_id = response.json()["job"]["id"]
    print(f" Job ID: {job_id}")

    return job_id


# -----------------------------
# TASK 2: Esperar finalización
# -----------------------------
@task
def wait_for_airbyte(job_id: int):
    url = f"{AIRBYTE_API_URL}/jobs/get"

    print(" Esperando finalización del sync...")

    while True:
        response = requests.post(url, json={"id": job_id})
        job = response.json()["job"]

        status = job["status"]
        print(f"Estado: {status}")

        if status == "succeeded":
            print(" Airbyte sync completado")
            break
        elif status in ["failed", "cancelled"]:
            raise Exception(" Airbyte sync falló")

        time.sleep(10)


# -----------------------------
# TASK 3: Ejecutar dbt run
# -----------------------------
@task
def run_dbt_models():
    print(" Ejecutando dbt run...")

    result = subprocess.run(
        ["dbt", "run"],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        raise Exception(f" Error en dbt run:\n{result.stderr}")

    print(" dbt run completado")


# -----------------------------
# TASK 4: Ejecutar dbt test
# -----------------------------
@task
def run_dbt_tests():
    print(" Ejecutando dbt test...")

    result = subprocess.run(
        ["dbt", "test"],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        raise Exception(f" Error en dbt test:\n{result.stderr}")

    print(" dbt test exitoso")


# -----------------------------
# FLOW PRINCIPAL
# -----------------------------
@flow(name="candy_data_pipeline")
def data_pipeline():
    job_id = run_airbyte_sync()
    wait_for_airbyte(job_id)
    run_dbt_models()
    run_dbt_tests()

    print(" Pipeline ejecutado correctamente")


# -----------------------------
# EJECUCIÓN
# -----------------------------
if __name__ == "__main__":
    data_pipeline()