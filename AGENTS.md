# AGENTS.md

## Project Purpose

This repository is a personal, production-oriented Data Engineering lab.

The goal is to practice building maintainable data pipelines using realistic engineering patterns while keeping the architecture simple enough to understand end-to-end.

## Main Stack

- Python
- Apache Airflow 3.3.1
- Docker Compose
- AWS S3
- Google BigQuery
- dbt (planned)
- Git / GitHub
- Codex as the AI coding agent

## Working Style

Follow this workflow:

1. Analyze the problem.
2. Explain the proposed approach.
3. Wait for human approval for architectural or significant design decisions.
4. Modify files only after approval.
5. Validate the implementation.
6. Report what changed and any remaining risks.

Do not make architectural decisions autonomously when multiple reasonable options exist.

Do not perform broad refactors unless explicitly requested.

Prefer simple, production-oriented solutions over unnecessary abstraction.

Preserve existing DAG IDs and established interfaces unless explicitly asked to change them.

Do not commit or push Git changes unless explicitly requested.

## Human-in-the-Loop Principle

The human developer owns:

- architecture decisions
- business semantics
- schema evolution decisions
- production trade-offs
- final code review

The coding agent may:

- inspect the repository
- analyze code
- propose solutions
- implement approved changes
- run validation commands
- inspect diffs
- help debug failures

## Airflow Conventions

- Airflow version: 3.3.1
- Use Airflow 3.x APIs such as `airflow.sdk`.
- Executor: LocalExecutor.
- Default project timezone: `Asia/Bangkok`.
- DAG schedules should be written directly in Bangkok local time.
- Use timezone-aware Pendulum datetimes.
- Avoid naive datetimes.
- Do not manually convert DAG schedules to UTC.
- Airflow internal/log timestamps may remain UTC.
- Use `catchup=False` for current-state polling pipelines unless historical backfill is explicitly required.

## Data Engineering Principles

### Raw Layer

Raw data should preserve the original source response whenever practical.

For Citi Bike GBFS feeds:

- store original JSON response bytes in S3
- raw storage is append-only by snapshot
- do not flatten or reserialize raw source data before storage

### Idempotency

Pipelines should support safe reruns where practical.

For snapshot ingestion, the logical grain is:

`(snapshot_at, station_id)`

Reprocessing the same snapshot must not create duplicate target rows.

## GBFS Specification and Schema Contract Policy

### Specification Baseline

The Citi Bike feeds currently ingested by this project declare:

`version = "1.1"`

Therefore, the ingestion-layer schema contracts for both:

- `station_status`
- `station_information`

must be validated against the GBFS v1.1 specification, not against the latest GBFS version.

The current GBFS specification may differ materially from v1.1. Do not treat a difference between the Citi Bike feed and the latest GBFS documentation as a source defect until the declared feed version has been checked.

If the source feed version changes from `1.1` to another version, the existing ingestion contract must not silently continue.

A version change requires:

1. validation failure
2. human review of the new GBFS version
3. comparison of the official schema with actual source payloads
4. explicit schema and code changes if required

### Source-of-Truth Hierarchy

Schema decisions should use both:

1. the official GBFS v1.1 specification
2. actual Citi Bike JSON payloads observed from the source

The official specification defines the standard contract.

Actual payloads are used to identify:

- provider-specific extensions
- optional fields that are not always present
- observed datatypes
- real-world null/missing behavior
- schema drift

Do not infer schema only from one source.

### Field Classification

Every source field should be classified as one of:

1. Official GBFS v1.1 field
2. Known Citi Bike / provider-specific extension
3. Unknown field not yet included in the project contract

### Required Official Fields

If an official field that is required by the active GBFS v1.1 contract is:

- missing
- null when not allowed
- of the wrong datatype

the ingestion must fail.

### Optional Official Fields

If an optional official field is absent:

- accept the record
- store NULL where applicable

If an optional field is present but has an invalid datatype:

- fail validation

### Known Provider Extensions

Citi Bike may include fields that are not part of the GBFS v1.1 standard.

Known provider extensions may be stored in BigQuery when they are useful and have been explicitly reviewed.

Provider-extension columns should generally remain nullable even if they are present in all currently observed records.

The pipeline must not depend on provider extensions for core GBFS validity.

### Unknown Fields

If a new field appears that is not in the current schema contract:

- emit a warning
- continue ingesting known columns
- preserve the complete original payload in S3
- do not automatically add a BigQuery column

Schema evolution requires human review.

### Historical Raw Preservation

Raw S3 objects preserve the original source response.

This allows:

- schema investigation
- future backfills
- replay after schema evolution
- comparison between provider behavior and the official specification

### Feed-Specific Validation

For both station feeds:

- feed version must equal `1.1`
- duplicate `station_id` within a snapshot must fail validation

For `station_status`:

- negative availability counts must fail validation
- status flags must conform to the expected GBFS v1.1 representation
- timestamp freshness is separate from structural schema validation

For `station_information`:

- `capacity < 0` must fail validation
- latitude outside `[-90, 90]` must fail validation
- longitude outside `[-180, 180]` must fail validation

## Citi Bike Conventions

Current feeds:

- `station_status`
- `station_information`

The `station_status` and `station_information` ingestion pipelines are complete for the current phase.

Current priority is dbt integration and building staging models on top of the proven ingestion tables.

### station_status

Polling frequency:

- every 10 minutes

S3 raw layout:

`raw/citibike/station_status/year=YYYY/month=MM/day=DD/station_status_YYYYMMDD_HHMMSS.json`

The timestamp in the object name represents the Airflow logical/scheduled time in Asia/Bangkok.

### station_information

Store `station_information` as daily historical snapshots rather than as a current-only dimension.

Polling frequency:

- daily at `0 6 * * *` in Asia/Bangkok

After a successful raw S3 upload, `citibike_station_information_poll`
automatically triggers `citibike_station_information_to_bq` and passes the exact
timezone-aware Bangkok snapshot timestamp through `dag_run.conf["snapshot_at"]`.

The following provider-specific fields are intentionally excluded from the
BigQuery ingestion contract by human decision:

- `eightd_station_services`
- `eightd_has_key_dispenser`

Do not validate or materialize these fields. If present, ignore them without
unknown-field warnings; the complete original payload remains preserved in S3.
The human developer has manually dropped both target columns. Do not recreate
them. Other unknown fields still follow the warning-and-continue policy.

The logical grain is:

`(snapshot_at, station_id)`

### BigQuery

GCP project:

`data-engineering-lab-nk`

Dataset:

`nyc_citibike`

Current BigQuery ingestion target tables:

- `data-engineering-lab-nk.nyc_citibike.station_status_snapshot`
- `data-engineering-lab-nk.nyc_citibike.station_information_snapshot`

The ingestion table keeps source-oriented field names.

Semantic transformations and renaming should be handled later in dbt staging models.

## Secrets

Never commit:

- `.env`
- AWS access keys
- Google service-account JSON
- passwords
- API tokens
- private credentials
- files under `secrets/`

Current GCP credential path inside Airflow containers:

`/opt/airflow/secrets/gcp-service-account.json`

## Git

Git is the source of truth for project code and project documentation.

Runtime-generated files such as Airflow logs and local secrets should not be committed.

Before significant changes:

- inspect current Git status
- understand existing code
- minimize unrelated modifications

After implementation:

- review diff
- validate changes
- commit only when requested

## Validation

For DAG changes, perform appropriate checks such as:

- Python syntax validation
- Airflow DAG parsing
- Docker Compose configuration validation when relevant
- task-level execution tests when practical

Do not report a change as successful unless the relevant validation has actually been performed.
