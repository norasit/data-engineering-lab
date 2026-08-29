# PROJECT_STATUS.md

## Current Checkpoint

Last updated: 2026-08-29

Development environment has been migrated from macOS to Ubuntu.

Ubuntu is now the active development/runtime environment for the lab.

The old Airflow Docker services on the MacBook have been stopped.

---

## Environment

### Local

- OS: Ubuntu Linux
- IDE: VS Code
- Containers: Docker + Docker Compose
- Airflow: 3.3.1
- Airflow executor: LocalExecutor
- Codex CLI available from the repository root

Repository:

`~/data-engineering-lab`

GitHub repository:

`norasit/data-engineering-lab`

Repository is private.

### Airflow

Core services:

- PostgreSQL
- airflow-apiserver
- airflow-scheduler
- airflow-dag-processor
- airflow-triggerer
- airflow-init
- airflow-cli

Project timezone:

`Asia/Bangkok`

---

## AWS

Personal AWS account is used for this lab.

S3 bucket:

`data-engineering-lab-nk`

Region:

`ap-southeast-1`

Raw Citi Bike data is being written successfully to S3.

Current station status path:

`raw/citibike/station_status/year=YYYY/month=MM/day=DD/`

Example:

`station_status_20260829_082000.json`

The Ubuntu Airflow runtime is now producing the scheduled S3 snapshots.

---

## GCP

GCP project:

`data-engineering-lab-nk`

BigQuery dataset:

`nyc_citibike`

Billing has been enabled and the project is linked to the billing account:

`Personal Data Engineering Lab`

BigQuery DML is now available.

GCP service-account authentication from Ubuntu has been verified successfully.

Credential file:

`secrets/gcp-service-account.json`

Container credential path:

`/opt/airflow/secrets/gcp-service-account.json`

---

# Citi Bike station_status Pipeline

## Source

NYC Citi Bike GBFS `station_status` feed.

Polling frequency:

every 10 minutes

The source is a current operational status feed, not a trip-event stream.

Historical state is created by periodically storing snapshots.

---

## DAG 1 - Raw Ingestion

File:

`dags/citibike_station_status.py`

DAG ID:

`citibike_station_status_poll`

Current flow:

GBFS API
-> Airflow
-> original JSON bytes
-> S3 raw snapshot

Schedule:

`*/10 * * * *`

Timezone:

`Asia/Bangkok`

Raw snapshots are working successfully.

---

## DAG 2 - BigQuery Ingestion

File:

`dags/citibike_station_status_to_bq.py`

DAG ID:

`citibike_station_status_to_bq`

Current schedule:

manual only (`schedule=None`)

Current flow:

S3 raw snapshot
-> schema validation
-> flatten stations
-> BigQuery staging table
-> validation of staging contents
-> transactional DELETE + INSERT
-> target snapshot table
-> drop staging table

Tasks:

1. `stage_validated_snapshot`
2. `replace_target_snapshot`
3. `drop_staging_table`

Target table:

`data-engineering-lab-nk.nyc_citibike.station_status_snapshot`

---

## Manual Snapshot Selection

DAG 2 accepts:

`dag_run.conf["snapshot_at"]`

Example:

```json
{
  "snapshot_at": "2026-08-29T08:20:00+07:00"
}
```

This value is authoritative for:

- S3 object selection
- snapshot_at
- snapshot_date
- staging table naming

The DAG does not search for the latest S3 object.

---

## BigQuery Validation Test

Snapshot tested:

`2026-08-29T08:20:00+07:00`

Equivalent UTC snapshot:

`2026-08-29 01:20:00 UTC`

Source object:

`station_status_20260829_082000.json`

All three DAG tasks completed successfully after GCP Billing was enabled.

Data was written successfully to:

`data-engineering-lab-nk.nyc_citibike.station_status_snapshot`

---

## Idempotency Test

Logical grain:

`(snapshot_at, station_id)`

Initial row count for the tested snapshot:

`2508`

The exact same snapshot was processed again.

Row count after rerun:

`2508`

Result:

PASS

The transactional snapshot replacement prevents duplicate rows when the same snapshot is reprocessed.

---

## station_status Schema Contract

Required source fields include:

- station_id
- num_bikes_available
- num_docks_available
- is_installed
- is_renting
- is_returning
- last_reported

Optional fields may be stored as NULL when absent.

Known Citi Bike extension fields currently supported:

- legacy_id
- eightd_has_available_keys
- num_ebikes_available
- num_scooters_available
- num_scooters_unavailable

Unknown source fields:

warn and continue ingesting known columns.

Do not automatically add new BigQuery columns.

Schema evolution requires human review.

---

# station_information

Raw ingestion DAG already exists and has been tested.

However, further work on `station_information` is intentionally paused until the `station_status` pipeline is completed.

Do not interleave additional station_information development yet.

---

# Current Milestone

The following station_status path is now proven:

GBFS API
-> Airflow
-> S3 raw JSON
-> validation
-> BigQuery staging
-> transactional target replacement
-> BigQuery target

Billing-related DML blocker is resolved.

Idempotent rerun has been verified.

---

# Next Steps

When development resumes:

1. Review whether DAG 1 should automatically trigger DAG 2.
2. If implemented, explicitly pass the upstream `snapshot_at` to DAG 2.
3. Keep DAG 2 without its own independent polling schedule.
4. Verify automatic end-to-end execution.
5. Begin dbt integration.
6. Create a dbt staging model for station status.
7. Add semantic type conversions and data-quality logic in dbt.
8. Return to `station_information` after station_status is complete.

Possible later improvements:

- staging table expiration fallback
- stronger automated tests
- CI/CD
- custom Docker image / pinned Python dependencies
- observability
- Terraform
- streaming extension as a separate future phase

Do not add these prematurely without a concrete learning or engineering need.
