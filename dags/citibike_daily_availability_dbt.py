import os
from pathlib import Path

import pendulum
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


NY_TZ = pendulum.timezone("America/New_York")
# The host Docker daemon resolves bind mounts against the host filesystem.
HOST_PROJECT_DIR = Path(os.environ["DBT_HOST_PROJECT_DIR"])
if not HOST_PROJECT_DIR.is_absolute():
    raise ValueError("DBT_HOST_PROJECT_DIR must be an absolute Ubuntu host path")


with DAG(
    dag_id="citibike_daily_availability_dbt",
    schedule="30 0 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz=NY_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["citibike", "dbt", "mart"],
) as dag:

    run_daily_availability = DockerOperator(
        task_id="run_daily_availability_incremental",
        image="citibike-dbt:1.12.4-bigquery1.12.0",
        # The image entrypoint is dbt.
        command=["run", "--select", "mart_citibike_daily_station_availability_incremental"],
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        force_pull=False,
        mount_tmp_dir=False,
        auto_remove="force",
        user=f"{os.environ.get('DBT_UID', '1000')}:{os.environ.get('DBT_GID', '1000')}",
        working_dir="/usr/app/dbt/citibike",
        mounts=[
            Mount(
                source=str(HOST_PROJECT_DIR / "dbt/citibike"),
                target="/usr/app/dbt/citibike",
                type="bind",
            ),
            Mount(
                source=str(HOST_PROJECT_DIR / "dbt/profiles"),
                target="/usr/app/dbt/profiles",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=str(HOST_PROJECT_DIR / "secrets/gcp-service-account.json"),
                target="/usr/app/secrets/gcp-service-account.json",
                type="bind",
                read_only=True,
            ),
        ],
        labels={"citibike.task": "daily_availability_dbt"},
        do_xcom_push=False,
    )
