# PROJECT_STATUS.md

## Current Checkpoint

Last updated: 2026-09-04

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

`station_status_20260904_052000.json`

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
-> trigger `citibike_station_status_to_bq`

Schedule:

`*/10 * * * *`

Timezone:

`Asia/Bangkok`

Raw snapshots are working successfully.

After the S3 upload succeeds, the DAG automatically triggers
`citibike_station_status_to_bq` and passes the exact upstream logical/scheduled
time through `dag_run.conf` as `snapshot_at`.

---

## DAG 2 - BigQuery Ingestion

File:

`dags/citibike_station_status_to_bq.py`

DAG ID:

`citibike_station_status_to_bq`

Current schedule:

no independent schedule (`schedule=None`)

The DAG is triggered automatically by `citibike_station_status_poll` after a
successful raw S3 upload. It can also be triggered manually for controlled
reprocessing.

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

## Snapshot Selection

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

For automatic runs, DAG 1 passes the same timezone-aware Bangkok timestamp used
to construct the S3 object key. This preserves one snapshot identity across the
raw S3 and BigQuery ingestion stages.

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

## Automatic End-to-End Validation Test

A real scheduled run was verified successfully.

Upstream scheduled snapshot:

`2026-09-04T05:20:00+07:00`

Source object:

`station_status_20260904_052000.json`

The upstream DAG completed its S3 upload and automatically triggered
`citibike_station_status_to_bq` at approximately `05:20:03 Asia/Bangkok`.

The downstream DAG received the exact upstream snapshot time through:

```json
{
  "snapshot_at": "2026-09-04T05:20:00+07:00"
}
```

The corresponding BigQuery `snapshot_at` is:

`2026-09-03 22:20:00 UTC`

BigQuery ingestion completed successfully for the same snapshot.

Result:

PASS

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

With the current `station_status` phase complete, `station_information` is now
the next main pipeline work item.

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

The automatic DAG 1 to DAG 2 handoff has been verified with a real scheduled
run. The exact snapshot identity was preserved from the Bangkok-local S3 object
key through the UTC BigQuery timestamp.

The `station_status` pipeline is complete end-to-end for the current phase.

---

# Next Steps

When development resumes:

1. Continue the `station_information` pipeline.
2. Complete and verify `station_information` end-to-end.
3. Begin dbt integration after the ingestion pipelines are ready.
4. Create dbt staging models for the Citi Bike datasets.
5. Add semantic type conversions and data-quality logic in dbt.

Possible later improvements:

- staging table expiration fallback
- stronger automated tests
- CI/CD
- custom Docker image / pinned Python dependencies
- observability
- Terraform
- streaming extension as a separate future phase

Do not add these prematurely without a concrete learning or engineering need.
