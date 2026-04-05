from prefect import flow, task
from prefect_airbyte.connections import AirbyteConnection
from prefect_dbt.cli.commands import DbtCoreOperation

@task
def run_airbyte():
    connection = AirbyteConnection(
        connection_id="AIRBYTE_CONNECTION_ID"
    )
    connection.sync()

@task
def run_dbt():
    dbt_run = DbtCoreOperation(
        commands=["dbt run", "dbt test"],
        project_dir="./dbt"
    )
    dbt_run.run()

@flow
def pipeline():
    run_airbyte()
    run_dbt()

if __name__ == "__main__":
    pipeline()