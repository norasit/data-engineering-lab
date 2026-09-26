# Citi Bike dbt container

The root `Dockerfile` builds only the dbt runtime from Python 3.12, with
`dbt-core==1.12.4` and `dbt-bigquery==1.12.0`. Airflow continues to use
`apache/airflow:3.3.1`; dbt is not installed in Airflow.

Run these commands from the repository root:

```bash
docker compose --profile dbt build dbt
docker compose --profile dbt run --rm dbt --version
docker compose --profile dbt run --rm dbt debug
docker compose --profile dbt run --rm dbt compile --select mart_citibike_daily_station_availability_incremental
docker compose --profile dbt run --rm dbt run --select mart_citibike_daily_station_availability_incremental
```

The `dbt` Compose profile keeps this one-off runtime out of a normal
`docker compose up`. The image entrypoint is `dbt`, so arguments follow the
service name directly. Run commands sequentially: they share project artifacts.

Runtime mounts:

| Host | Container | Access |
| --- | --- | --- |
| `./dbt/citibike` | `/usr/app/dbt/citibike` | Read/write |
| `./dbt/profiles` | `/usr/app/dbt/profiles` | Read-only |
| `./secrets/gcp-service-account.json` | `/usr/app/secrets/gcp-service-account.json` | Read-only |

The container profile selects `citibike` / `dev`, project
`data-engineering-lab-nk`, dataset `nyc_citibike_dbt`, location
`asia-southeast1`, and four threads. Host `~/.dbt/profiles.yml` is unchanged.
The Docker build context excludes everything except the Dockerfile and
`.dockerignore`; no credentials or project files are copied into the image.

The runtime uses UID/GID `1000:1000`, matching this project's host owner.
Override `DBT_UID` and `DBT_GID` if the host project owner differs. Generated
`dbt/citibike/logs/` and `dbt/citibike/target/` stay on the host and are ignored
by Git, as are credentials.

## Airflow orchestration

`citibike_daily_availability_dbt` uses DockerOperator to start a fresh container
from `citibike-dbt:1.12.4-bigquery1.12.0` on each run. It executes
`dbt run --select mart_citibike_daily_station_availability_incremental` with
the mounts above. The schedule remains 00:30 America/New_York daily, with
`catchup=False` and `max_active_runs=1`.

DockerOperator streams dbt output into Airflow task logs and fails the task on
a nonzero exit code. `auto_remove="force"` removes the container on success
or failure during normal operator execution. A host/scheduler crash can still
leave an orphan; inspect containers with label
`citibike.task=daily_availability_dbt` before removing any abandoned container.
The dbt artifact directories remain available after container removal.
`mount_tmp_dir=False` avoids trying to mount the scheduler's private temporary
directory through the host daemon. No persistent dbt service is started.

Only `airflow-scheduler` mounts `/var/run/docker.sock`, because LocalExecutor
runs tasks there. Its supplementary group defaults to this host's Docker
group, `984`. Socket access grants scheduler tasks effective root-level control
of the host through the Docker API. The dbt container, API server, DAG
processor, triggerer, init and CLI services do not receive the socket. This
setup assumes trusted local DAG code.

`DBT_HOST_PROJECT_DIR` must identify the absolute repository path on the host.
It defaults to the shell's `PWD`, so run Compose from the repository root.
When running elsewhere, set `DBT_HOST_PROJECT_DIR` explicitly. The scheduler
and DAG processor must receive the same value because Docker bind mounts
refer to host paths, not Airflow container paths. On a different host, set
`DOCKER_GID` to the output of `stat -c '%g' /var/run/docker.sock` and adjust
`DBT_UID` / `DBT_GID` to match the project owner.

After building the dbt image and changing orchestration settings:

```bash
docker compose config --quiet
docker compose up -d --no-deps airflow-scheduler airflow-dag-processor
docker compose exec airflow-scheduler airflow dags unpause citibike_daily_availability_dbt
docker compose exec airflow-scheduler airflow dags trigger citibike_daily_availability_dbt
```

Enable the DAG in Airflow for daily scheduling. Avoid running host/Compose dbt
commands concurrently with this DAG: `max_active_runs` only serializes Airflow
runs, and all runtimes share the same artifact paths and BigQuery target.

## Validation on 2026-09-26

Compose validation and all five DAG imports passed. The manual run
`manual__dockeroperator_validation_20260926T1416` finished successfully, with
task `run_daily_availability_incremental` successful and dbt reporting
`PASS=1 WARN=0 ERROR=0`. Its merge affected zero rows and processed 20.3 MiB.
Docker lifecycle events confirmed create/start, exit code 0, and destruction;
no containers with the task label remained afterward.

Temporarily unpausing also released the latest scheduled run
`scheduled__2026-09-26T04:30:00+00:00`, which succeeded and cleaned up its
container. The DAG was restored to its original paused state after validation.
Unpause it when ready to enable daily scheduling. Existing ingestion DAGs were
not changed; the transformation SQL checksum was unchanged.
