# PROJECT_STATUS.md

## Current Checkpoint

Last updated: 2026-09-08

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

# GBFS Schema Baseline

## Observation Date

The current schema assessment is based on:

- official GBFS v1.1 documentation
- actual Citi Bike raw JSON snapshots observed through early September 2026
- most recent reviewed source files dated 2026-09-04

The current source payloads for both station feeds declare:

`version = "1.1"`

Therefore, the current ingestion contracts are explicitly based on GBFS v1.1.

GBFS v1.1 is an older/deprecated specification version, but it remains the correct baseline for these Citi Bike payloads because the source itself declares version 1.1.

Do not redesign the ingestion schema against the latest GBFS version unless the source feed version changes.

---

# station_status Schema Contract

## Purpose

`station_status` represents operational station availability at a point in time.

The project polls this feed every 10 minutes and stores historical snapshots.

Target table:

`data-engineering-lab-nk.nyc_citibike.station_status_snapshot`

Grain:

`(snapshot_at, station_id)`

## Snapshot Metadata

Project-generated / envelope fields:

- `snapshot_at`
- `snapshot_date`
- `ingested_at`
- `source_object_uri`

Feed-level fields:

- `version`
- `last_updated`
- `ttl`

## Official GBFS v1.1 Station Fields

The current contract includes the standard station-status fields used by the Citi Bike v1.1 payload:

- `station_id`
- `num_bikes_available`
- `num_bikes_disabled`
- `num_docks_available`
- `num_docks_disabled`
- `is_installed`
- `is_renting`
- `is_returning`
- `last_reported`

Core required fields are validated strictly.

Optional fields are nullable.

## Citi Bike Provider Extensions

The current source also includes known Citi Bike-specific fields:

- `legacy_id`
- `eightd_has_available_keys`
- `num_ebikes_available`
- `num_scooters_available`
- `num_scooters_unavailable`

These fields are not treated as core GBFS validity requirements.

They are stored because they are present in the actual provider payload and may be useful analytically.

Provider-extension fields are nullable where appropriate.

## Early September 2026 Observations

A multi-snapshot review showed:

- stable station-level schema across the reviewed snapshots
- no datatype drift in the known fields
- `station_id` unique within each snapshot
- scooter-related fields are not present on every record
- missing optional/provider fields are stored as NULL
- `last_reported` may contain structurally valid but operationally stale values

Timestamp freshness is treated separately from structural schema validation.

## Validation Policy

- feed version must equal `1.1`
- missing required official field -> fail
- invalid required datatype -> fail
- missing optional field -> NULL
- invalid optional datatype when present -> fail
- unknown field -> warning, continue ingesting known fields
- duplicate station_id within a snapshot -> fail
- negative availability counts -> fail
- status flags must conform to the expected v1.1 representation

The complete raw JSON remains preserved in S3.

---

# station_information Schema Contract

## Purpose

`station_information` represents relatively stable station reference metadata such as:

- identity
- public name
- coordinates
- station capacity
- supported rental methods

The feed is ingested as a daily historical snapshot rather than as a current-only dimension.

Target table:

`data-engineering-lab-nk.nyc_citibike.station_information_snapshot`

The `station_information_snapshot` table has been created manually in BigQuery.

The S3-to-BigQuery DAG is implemented in:

`dags/citibike_station_information_to_bq.py`

DAG ID:

`citibike_station_information_to_bq`

It has no independent schedule (`schedule=None`).

The raw ingestion DAG is:

`dags/citibike_station_information.py`

DAG ID:

`citibike_station_information_poll`

Schedule:

`0 6 * * *`

Timezone:

`Asia/Bangkok`

`catchup=False`

Current end-to-end flow:

GBFS API
-> Airflow
-> original JSON bytes
-> S3 raw snapshot
-> automatic trigger
-> `citibike_station_information_to_bq`
-> schema validation
-> BigQuery staging table
-> staging validation
-> transactional DELETE + INSERT
-> `station_information_snapshot`
-> drop staging table

The raw upload task returns the exact timezone-aware Bangkok logical/scheduled
timestamp used to construct the S3 object key. The downstream trigger passes
that same value through `dag_run.conf["snapshot_at"]`.

Neither DAG searches for the latest S3 object.

## station_information Validation Results

Manual S3-to-BigQuery validation passed successfully on 2026-09-08.

Verified:

- all three downstream tasks completed successfully
- data was loaded into `station_information_snapshot`
- `snapshot_at`, `snapshot_date`, and source-object identity were consistent
- the staging table was cleaned up after successful replacement
- rerunning the exact same snapshot did not create duplicate target rows

Idempotency result:

PASS

The automatic DAG 1 to DAG 2 handoff was also tested successfully by manually
triggering `citibike_station_information_poll`.

Verified:

- a new raw JSON object was written to S3
- `citibike_station_information_to_bq` was triggered automatically
- new snapshot data was written successfully to BigQuery

Automatic handoff result:

PASS

A naturally scheduled `06:00 Asia/Bangkok` run has not yet been separately
verified, but the complete trigger path has been proven through a real manual
upstream DAG run using live S3 and BigQuery resources.

Grain:

`(snapshot_at, station_id)`

## Snapshot Metadata

Project-generated / envelope fields:

- `snapshot_at`
- `snapshot_date`
- `ingested_at`
- `source_object_uri`

Feed-level fields:

- `version`
- `last_updated`
- `ttl`

## Official GBFS v1.1 Fields Included in the Current Contract

- `station_id`
- `name`
- `short_name`
- `lat`
- `lon`
- `region_id`
- `capacity`
- `rental_methods`
- `rental_uris`

Observed behavior through early September 2026:

- `station_id` is present and unique within each reviewed snapshot
- `name`, `lat`, and `lon` are consistently present
- `region_id` is not present on every station record
- `capacity` is observed as a non-negative integer
- `rental_methods` is an array of strings
- `rental_uris` is an object containing provider deep links

The BigQuery contract therefore keeps `region_id` nullable.

## rental_uris Representation

The target table stores:

- `android`
- `ios`
- `web`

inside a BigQuery STRUCT.

The currently observed Citi Bike files primarily populate Android and iOS links.

The `web` field is included because it belongs to the recognized rental URI structure even though it is not currently populated in the reviewed samples.

## Citi Bike Provider Extensions

The BigQuery ingestion contract retains these reviewed provider-specific fields:

- `has_kiosk`
- `station_type`
- `external_id`
- `electric_bike_surcharge_waiver`

These fields are retained because they exist in the real Citi Bike payload.

They are not treated as required GBFS v1.1 fields and are nullable in the target contract.

### Intentionally Excluded Provider Fields

On 2026-09-08, the human developer manually dropped these target columns and
approved excluding them from the BigQuery ingestion contract:

- `eightd_station_services`
- `eightd_has_key_dispenser`

Neither field is validated or materialized. When present in raw JSON, both are
ignored without unknown-field warnings. Their original values remain preserved
in the unchanged raw S3 payload. Other unknown fields still warn and continue.
No BigQuery writes or DDL are part of this code and documentation update.

The reviewed 2026-09-04 snapshot contains eight stations with service objects
inside `eightd_station_services`, which did not match its former REPEATED STRING
column. Excluding this field removes that ingestion schema mismatch.

## Fields Not Yet Materialized

Some optional GBFS fields may exist in the specification but have not been observed in the reviewed Citi Bike payloads.

The project does not automatically create BigQuery columns for every optional field in the specification.

If a currently unmodeled field appears:

1. raw S3 preserves it
2. ingestion logs a warning
3. known fields continue to load
4. human review determines whether the BigQuery schema should evolve

## Early September 2026 Observations

Four reviewed station_information snapshots:

- 2026-08-28
- 2026-08-29
- 2026-09-03
- 2026-09-04

showed approximately 2.5k stations per snapshot.

The station count changed slightly between snapshots, confirming that station_information should not be treated as a perfectly static master.

The project therefore stores historical daily snapshots.

## Validation Policy

- feed version must equal `1.1`
- missing required official field -> fail
- invalid required datatype -> fail
- missing optional official field -> NULL
- invalid optional datatype when present -> fail
- missing known provider extension -> NULL
- unknown field -> warning, continue ingesting known fields
- duplicate station_id within a snapshot -> fail
- capacity < 0 -> fail
- latitude outside [-90, 90] -> fail
- longitude outside [-180, 180] -> fail

The complete raw JSON remains preserved in S3.

---

# Current Milestone

Both Citi Bike ingestion pipelines are now proven end-to-end for the current phase:

- `station_status`
- `station_information`

Common proven pattern:

GBFS API
-> Airflow
-> S3 raw JSON
-> automatic DAG handoff
-> validation
-> BigQuery staging
-> transactional target replacement
-> BigQuery snapshot target
-> staging cleanup

Billing-related DML blocker is resolved.

Idempotent reruns have been verified for both snapshot pipelines.

For `station_status`, the automatic handoff has been verified with a real
scheduled run.

For `station_information`, the automatic handoff has been verified with a real
manual upstream DAG run using live S3 and BigQuery resources. The natural
`06:00 Asia/Bangkok` scheduled run has not yet been separately observed.

The ingestion layer is ready for the next phase: dbt integration.

---

# Next Steps

When development resumes:

1. Begin dbt integration.
2. Create dbt staging models for `station_status_snapshot` and `station_information_snapshot`.
3. Add semantic type conversions and data-quality logic in dbt.
4. Define simple downstream models only after the staging layer is clear and tested.
5. Optionally observe one natural `06:00 Asia/Bangkok` station_information run as an additional operational check.

Possible later improvements:

- staging table expiration fallback
- stronger automated tests
- CI/CD
- custom Docker image / pinned Python dependencies
- observability
- Terraform
- streaming extension as a separate future phase

Do not add these prematurely without a concrete learning or engineering need.
