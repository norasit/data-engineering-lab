import logging
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

import boto3
import pendulum
from airflow.sdk import dag, get_current_context, task
from google.cloud import bigquery


S3_BUCKET = "data-engineering-lab-nk"
S3_PREFIX = "raw/citibike/station_status"
GCP_PROJECT = "data-engineering-lab-nk"
BIGQUERY_DATASET = "nyc_citibike"
TARGET_TABLE = (
    "data-engineering-lab-nk.nyc_citibike.station_status_snapshot"
)
BANGKOK_TIMEZONE = pendulum.timezone("Asia/Bangkok")
LOGGER = logging.getLogger(__name__)

ROOT_FIELDS = {"version", "last_updated", "ttl", "data"}
DATA_FIELDS = {"stations"}
REQUIRED_STATION_FIELDS = {
    "station_id",
    "num_bikes_available",
    "num_docks_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
}
OPTIONAL_STANDARD_FIELDS = {
    "num_bikes_disabled",
    "num_docks_disabled",
}
EXTENSION_FIELDS = {
    "legacy_id",
    "eightd_has_available_keys",
    "num_ebikes_available",
    "num_scooters_available",
    "num_scooters_unavailable",
}
KNOWN_STATION_FIELDS = (
    REQUIRED_STATION_FIELDS | OPTIONAL_STANDARD_FIELDS | EXTENSION_FIELDS
)
NON_NEGATIVE_INTEGER_FIELDS = {
    "num_bikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
    "num_ebikes_available",
    "num_scooters_available",
    "num_scooters_unavailable",
}
STATUS_FIELDS = {"is_installed", "is_renting", "is_returning"}

STAGING_SCHEMA = [
    bigquery.SchemaField("snapshot_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("source_object_uri", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("version", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("last_updated", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("ttl", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("num_bikes_available", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("num_bikes_disabled", "INT64"),
    bigquery.SchemaField("num_docks_available", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("num_docks_disabled", "INT64"),
    bigquery.SchemaField("is_installed", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("is_renting", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("is_returning", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("last_reported", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("legacy_id", "STRING"),
    bigquery.SchemaField("eightd_has_available_keys", "BOOL"),
    bigquery.SchemaField("num_ebikes_available", "INT64"),
    bigquery.SchemaField("num_scooters_available", "INT64"),
    bigquery.SchemaField("num_scooters_unavailable", "INT64"),
]


def _is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _require_integer(container: dict[str, Any], field: str, location: str) -> int:
    if field not in container or container[field] is None:
        raise ValueError(f"{location}.{field} is required and must not be null")
    value = container[field]
    if not _is_integer(value):
        raise ValueError(f"{location}.{field} must be an integer")
    return value


def _optional_integer(
    station: dict[str, Any], field: str, location: str
) -> int | None:
    if field not in station:
        return None
    value = station[field]
    if value is None or not _is_integer(value):
        raise ValueError(f"{location}.{field} must be an integer when present")
    return value


def _nullable_extension(
    station: dict[str, Any], field: str, expected_type: type, location: str
) -> Any:
    value = station.get(field)
    if value is None:
        return None
    if expected_type is int:
        if not _is_integer(value):
            raise ValueError(f"{location}.{field} must be an integer or null")
    elif not isinstance(value, expected_type):
        raise ValueError(
            f"{location}.{field} must be {expected_type.__name__} or null"
        )
    return value


def _warn_for_unknown_fields(
    level: str,
    unknown_fields: dict[str, Counter[str]],
    records_inspected: int,
) -> None:
    for field in sorted(unknown_fields):
        type_counts = dict(sorted(unknown_fields[field].items()))
        LOGGER.warning(
            "schema_drift level=%s field=%s observed_types=%s occurrences=%s "
            "records_inspected=%s action=ignored_known_columns_only",
            level,
            field,
            type_counts,
            sum(type_counts.values()),
            records_inspected,
        )


@dag(
    dag_id="citibike_station_status_to_bq",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 28, tz=BANGKOK_TIMEZONE),
    catchup=False,
    max_active_runs=1,
)
def citibike_station_status_to_bq():
    @task(retries=1)
    def stage_validated_snapshot() -> dict[str, Any]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_run_conf = dag_run.conf if dag_run is not None and dag_run.conf else {}

        if "snapshot_at" in dag_run_conf:
            configured_snapshot_at = dag_run_conf["snapshot_at"]
            if not isinstance(configured_snapshot_at, str):
                raise ValueError("dag_run.conf['snapshot_at'] must be a string")
            try:
                parsed_snapshot_at = datetime.fromisoformat(configured_snapshot_at)
            except ValueError as error:
                raise ValueError(
                    "dag_run.conf['snapshot_at'] must be a valid ISO 8601 timestamp"
                ) from error
            if (
                parsed_snapshot_at.tzinfo is None
                or parsed_snapshot_at.utcoffset() is None
            ):
                raise ValueError(
                    "dag_run.conf['snapshot_at'] must include a timezone offset"
                )
            snapshot_at_bangkok = pendulum.instance(
                parsed_snapshot_at
            ).in_timezone(BANGKOK_TIMEZONE)
        else:
            logical_date = context.get("logical_date")
            if logical_date is None:
                raise ValueError(
                    "DAG run must have a logical date when snapshot_at is not configured"
                )
            if logical_date.tzinfo is None or logical_date.utcoffset() is None:
                raise ValueError("DAG run logical_date must be timezone-aware")
            snapshot_at_bangkok = pendulum.instance(logical_date).in_timezone(
                BANGKOK_TIMEZONE
            )

        snapshot_at = snapshot_at_bangkok.in_timezone("UTC")
        snapshot_date = snapshot_at_bangkok.date()
        snapshot_at_serialized = snapshot_at.isoformat()
        snapshot_date_serialized = snapshot_date.isoformat()
        timestamp_token = snapshot_at_bangkok.format("YYYYMMDD_HHmmss")
        s3_key = (
            f"{S3_PREFIX}/year={snapshot_at_bangkok:%Y}/"
            f"month={snapshot_at_bangkok:%m}/day={snapshot_at_bangkok:%d}/"
            f"station_status_{timestamp_token}.json"
        )
        source_object_uri = f"s3://{S3_BUCKET}/{s3_key}"
        staging_table_id = (
            f"{GCP_PROJECT}.{BIGQUERY_DATASET}."
            f"_load_station_status_{timestamp_token}"
        )

        response = boto3.client("s3").get_object(Bucket=S3_BUCKET, Key=s3_key)
        source_bytes = response["Body"].read()

        try:
            import json

            payload = json.loads(source_bytes)
        except (UnicodeDecodeError, ValueError) as error:
            raise ValueError("S3 object must contain valid JSON") from error

        if not isinstance(payload, dict):
            raise ValueError("JSON root must be an object")

        root_unknown: dict[str, Counter[str]] = defaultdict(Counter)
        for field, value in payload.items():
            if field not in ROOT_FIELDS:
                root_unknown[field][_json_type(value)] += 1

        version = payload.get("version")
        if not isinstance(version, str):
            raise ValueError("version is required and must be a string")
        if version != "1.1":
            raise ValueError(f"version must equal '1.1'; received {version!r}")
        last_updated = _require_integer(payload, "last_updated", "root")
        ttl = _require_integer(payload, "ttl", "root")

        if "data" not in payload or not isinstance(payload["data"], dict):
            raise ValueError("data is required and must be an object")
        data = payload["data"]

        data_unknown: dict[str, Counter[str]] = defaultdict(Counter)
        for field, value in data.items():
            if field not in DATA_FIELDS:
                data_unknown[field][_json_type(value)] += 1

        if "stations" not in data or not isinstance(data["stations"], list):
            raise ValueError("data.stations is required and must be an array")
        stations = data["stations"]

        station_unknown: dict[str, Counter[str]] = defaultdict(Counter)
        station_ids: set[str] = set()
        rows: list[dict[str, Any]] = []

        for index, station in enumerate(stations):
            location = f"data.stations[{index}]"
            if not isinstance(station, dict):
                raise ValueError(f"{location} must be an object")

            for field, value in station.items():
                if field not in KNOWN_STATION_FIELDS:
                    station_unknown[field][_json_type(value)] += 1

            if "station_id" not in station or station["station_id"] is None:
                raise ValueError(f"{location}.station_id is required")
            station_id = station["station_id"]
            if not isinstance(station_id, str) or not station_id.strip():
                raise ValueError(f"{location}.station_id must be a non-empty string")
            if station_id in station_ids:
                raise ValueError(f"duplicate station_id in snapshot: {station_id!r}")
            station_ids.add(station_id)

            required_integers = {
                field: _require_integer(station, field, location)
                for field in REQUIRED_STATION_FIELDS - {"station_id"}
            }
            optional_integers = {
                field: _optional_integer(station, field, location)
                for field in OPTIONAL_STANDARD_FIELDS
            }

            all_integer_values = {**required_integers, **optional_integers}
            for field in NON_NEGATIVE_INTEGER_FIELDS & all_integer_values.keys():
                value = all_integer_values[field]
                if value is not None and value < 0:
                    raise ValueError(f"{location}.{field} must be non-negative")
            for field in STATUS_FIELDS:
                if required_integers[field] not in {0, 1}:
                    raise ValueError(f"{location}.{field} must be 0 or 1")

            legacy_id = _nullable_extension(station, "legacy_id", str, location)
            has_keys = _nullable_extension(
                station, "eightd_has_available_keys", bool, location
            )
            extension_integers = {
                field: _nullable_extension(station, field, int, location)
                for field in EXTENSION_FIELDS
                - {"legacy_id", "eightd_has_available_keys"}
            }
            for field, value in extension_integers.items():
                if value is not None and value < 0:
                    raise ValueError(f"{location}.{field} must be non-negative")

            rows.append(
                {
                    "snapshot_at": snapshot_at_serialized,
                    "snapshot_date": snapshot_date_serialized,
                    "source_object_uri": source_object_uri,
                    "version": version,
                    "last_updated": last_updated,
                    "ttl": ttl,
                    "station_id": station_id,
                    **required_integers,
                    **optional_integers,
                    "legacy_id": legacy_id,
                    "eightd_has_available_keys": has_keys,
                    **extension_integers,
                }
            )

        _warn_for_unknown_fields("root", root_unknown, 1)
        _warn_for_unknown_fields("data", data_unknown, 1)
        _warn_for_unknown_fields("station", station_unknown, len(stations))

        client = bigquery.Client(project=GCP_PROJECT)
        job_config = bigquery.LoadJobConfig(
            schema=STAGING_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_json(
            rows,
            staging_table_id,
            job_config=job_config,
        )
        load_job.result()

        LOGGER.info(
            "staged snapshot_at=%s source_object_uri=%s source_byte_size=%s "
            "station_count=%s validated_unique_station_count=%s "
            "staging_table_id=%s bigquery_load_job_id=%s",
            snapshot_at.isoformat(),
            source_object_uri,
            len(source_bytes),
            len(stations),
            len(station_ids),
            staging_table_id,
            load_job.job_id,
        )

        return {
            "staging_table_id": staging_table_id,
            "snapshot_at": snapshot_at_serialized,
            "snapshot_date": snapshot_date_serialized,
            "source_object_uri": source_object_uri,
            "validated_row_count": len(rows),
        }

    @task(retries=1)
    def replace_target_snapshot(metadata: dict[str, Any]) -> dict[str, Any]:
        client = bigquery.Client(project=GCP_PROJECT)
        staging_table_id = metadata["staging_table_id"]
        snapshot_at = pendulum.parse(metadata["snapshot_at"])
        expected_count = metadata["validated_row_count"]
        query_parameters = [
            bigquery.ScalarQueryParameter("snapshot_at", "TIMESTAMP", snapshot_at)
        ]

        verification_sql = f"""
            SELECT
              COUNT(*) AS row_count,
              COUNT(DISTINCT snapshot_at) AS distinct_snapshot_count,
              COUNT(*) - COUNT(DISTINCT CONCAT(
                FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%Ez', snapshot_at),
                '\\x1f',
                station_id
              )) AS duplicate_key_count
            FROM `{staging_table_id}`
            WHERE snapshot_at = @snapshot_at
        """
        verification_job = client.query(
            verification_sql,
            job_config=bigquery.QueryJobConfig(query_parameters=query_parameters),
        )
        verification = next(iter(verification_job.result()))
        if verification.row_count != expected_count:
            raise ValueError(
                "staging row count does not equal validated row count: "
                f"{verification.row_count} != {expected_count}"
            )
        if verification.distinct_snapshot_count != 1:
            raise ValueError("staging must contain exactly one distinct snapshot_at")
        if verification.duplicate_key_count != 0:
            raise ValueError("staging contains duplicate snapshot_at/station_id keys")

        replacement_sql = f"""
            BEGIN TRANSACTION;

            DELETE FROM `{TARGET_TABLE}`
            WHERE snapshot_at = @snapshot_at;

            INSERT INTO `{TARGET_TABLE}` (
              snapshot_at,
              snapshot_date,
              ingested_at,
              source_object_uri,
              version,
              last_updated,
              ttl,
              station_id,
              num_bikes_available,
              num_bikes_disabled,
              num_docks_available,
              num_docks_disabled,
              is_installed,
              is_renting,
              is_returning,
              last_reported,
              legacy_id,
              eightd_has_available_keys,
              num_ebikes_available,
              num_scooters_available,
              num_scooters_unavailable
            )
            SELECT
              snapshot_at,
              snapshot_date,
              CURRENT_TIMESTAMP() AS ingested_at,
              source_object_uri,
              version,
              last_updated,
              ttl,
              station_id,
              num_bikes_available,
              num_bikes_disabled,
              num_docks_available,
              num_docks_disabled,
              is_installed,
              is_renting,
              is_returning,
              last_reported,
              legacy_id,
              eightd_has_available_keys,
              num_ebikes_available,
              num_scooters_available,
              num_scooters_unavailable
            FROM `{staging_table_id}`
            WHERE snapshot_at = @snapshot_at;

            COMMIT TRANSACTION;
        """
        replacement_job = client.query(
            replacement_sql,
            job_config=bigquery.QueryJobConfig(query_parameters=query_parameters),
        )
        replacement_job.result()

        count_sql = f"""
            SELECT COUNT(*) AS inserted_row_count
            FROM `{TARGET_TABLE}`
            WHERE snapshot_at = @snapshot_at
        """
        count_job = client.query(
            count_sql,
            job_config=bigquery.QueryJobConfig(query_parameters=query_parameters),
        )
        inserted_row_count = next(iter(count_job.result())).inserted_row_count
        if inserted_row_count != expected_count:
            raise RuntimeError(
                "committed target row count does not equal validated row count: "
                f"{inserted_row_count} != {expected_count}"
            )

        LOGGER.info(
            "replaced snapshot_at=%s source_object_uri=%s "
            "target_replacement_query_job_id=%s final_inserted_row_count=%s",
            metadata["snapshot_at"],
            metadata["source_object_uri"],
            replacement_job.job_id,
            inserted_row_count,
        )
        return metadata

    @task(retries=1, do_xcom_push=False)
    def drop_staging_table(metadata: dict[str, Any]) -> None:
        staging_table_id = metadata["staging_table_id"]
        try:
            bigquery.Client(project=GCP_PROJECT).delete_table(
                staging_table_id,
                not_found_ok=True,
            )
        except Exception:
            LOGGER.exception(
                "staging cleanup failed staging_table_id=%s; "
                "committed target data was not changed",
                staging_table_id,
            )
            raise
        LOGGER.info("dropped staging_table_id=%s", staging_table_id)

    staged_metadata = stage_validated_snapshot()
    replaced_metadata = replace_target_snapshot(staged_metadata)
    drop_staging_table(replaced_metadata)


citibike_station_status_to_bq()
