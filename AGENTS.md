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

### Schema Evolution

Do not automatically evolve downstream schemas when an unknown source field appears.

Current policy:

- required field missing/null -> fail
- required datatype mismatch -> fail
- optional field missing -> accept as NULL
- optional datatype mismatch when present -> fail
- unknown field -> warn and continue ingesting known fields

Raw S3 data remains the recovery source for future schema changes and backfills.

Schema changes require human review.

### Idempotency

Pipelines should support safe reruns where practical.

For snapshot ingestion, the logical grain is:

`(snapshot_at, station_id)`

Reprocessing the same snapshot must not create duplicate target rows.

## Citi Bike Conventions

Current feeds:

- `station_status`
- `station_information`

Current priority is to complete `station_status` end-to-end before continuing work on `station_information`.

### station_status

Polling frequency:

- every 10 minutes

S3 raw layout:

`raw/citibike/station_status/year=YYYY/month=MM/day=DD/station_status_YYYYMMDD_HHMMSS.json`

The timestamp in the object name represents the Airflow logical/scheduled time in Asia/Bangkok.

### BigQuery

GCP project:

`data-engineering-lab-nk`

Dataset:

`nyc_citibike`

Current station status target table:

`data-engineering-lab-nk.nyc_citibike.station_status_snapshot`

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
