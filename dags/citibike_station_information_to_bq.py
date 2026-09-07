import logging
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

import boto3
import pendulum
from airflow.sdk import dag, get_current_context, task
from google.cloud import bigquery


S3_BUCKET = "data-engineering-lab-nk"
S3_PREFIX = "raw/citibike/station_information"
GCP_PROJECT = "data-engineering-lab-nk"
BIGQUERY_DATASET = "nyc_citibike"
TARGET_TABLE = (
    "data-engineering-lab-nk.nyc_citibike.station_information_snapshot"
)
BANGKOK_TIMEZONE = pendulum.timezone("Asia/Bangkok")
LOGGER = logging.getLogger(__name__)

ROOT_FIELDS = {"version", "last_updated", "ttl", "data"}
DATA_FIELDS = {"stations"}
# Modeled fields from the GBFS v1.1 specification and reviewed Citi Bike payloads.
REQUIRED_STATION_FIELDS = {"station_id", "name", "lat", "lon"}
OPTIONAL_STANDARD_FIELDS = {
    "short_name", "region_id", "capacity", "rental_methods", "rental_uris",
}
EXTENSION_FIELDS = {
    "has_kiosk", "station_type", "external_id",
    "electric_bike_surcharge_waiver",
}
# Human-approved exclusions: preserve only in raw S3, without validation.
IGNORED_PROVIDER_FIELDS = {"eightd_station_services", "eightd_has_key_dispenser"}
KNOWN_STATION_FIELDS = (
    REQUIRED_STATION_FIELDS | OPTIONAL_STANDARD_FIELDS | EXTENSION_FIELDS
    | IGNORED_PROVIDER_FIELDS
)
RENTAL_URI_FIELDS = {"android", "ios", "web"}

STAGING_SCHEMA = [
    bigquery.SchemaField("snapshot_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("source_object_uri", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("version", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("last_updated", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("ttl", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("short_name", "STRING"),
    bigquery.SchemaField("lat", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("lon", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("region_id", "STRING"),
    bigquery.SchemaField("capacity", "INT64"),
    # Existing target arrays cannot retain NULL: missing arrays become [].
    bigquery.SchemaField("rental_methods", "STRING", mode="REPEATED"),
    bigquery.SchemaField(
        "rental_uris", "RECORD",
        fields=[
            bigquery.SchemaField("android", "STRING"),
            bigquery.SchemaField("ios", "STRING"),
            bigquery.SchemaField("web", "STRING"),
        ],
    ),
    bigquery.SchemaField("has_kiosk", "BOOL"),
    bigquery.SchemaField("station_type", "STRING"),
    bigquery.SchemaField("external_id", "STRING"),
    bigquery.SchemaField("electric_bike_surcharge_waiver", "BOOL"),
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


def _optional_string(
    container: dict[str, Any], field: str, location: str
) -> str | None:
    if field not in container:
        return None
    value = container[field]
    if not isinstance(value, str):
        raise ValueError(f"{location}.{field} must be a string when present")
    return value


def _nullable_extension(
    station: dict[str, Any], field: str, expected_type: type, location: str
) -> Any:
    value = station.get(field)
    if value is not None and not isinstance(value, expected_type):
        raise ValueError(
            f"{location}.{field} must be {expected_type.__name__} or null"
        )
    return value


def _string_array(
    station: dict[str, Any], field: str, location: str
) -> list[str]:
    # BigQuery REPEATED columns use [] for absent collections.
    # The original distinction remains in S3.
    if field not in station:
        return []
    value = station[field]
    if not isinstance(value, list) or any(
        not isinstance(item, str) for item in value
    ):
        raise ValueError(f"{location}.{field} must be an array of strings")
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
    dag_id="citibike_station_information_to_bq",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 28, tz=BANGKOK_TIMEZONE),
    catchup=False,
    max_active_runs=1,
)
def citibike_station_information_to_bq():
    @task(retries=1)
    def stage_validated_snapshot() -> dict[str, Any]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_run_conf = dag_run.conf if dag_run is not None and dag_run.conf else {}

        if "snapshot_at" not in dag_run_conf:
            raise ValueError("dag_run.conf['snapshot_at'] is required")
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

        snapshot_at = snapshot_at_bangkok.in_timezone("UTC")
        snapshot_date = snapshot_at_bangkok.date()
        snapshot_at_serialized = snapshot_at.isoformat()
        snapshot_date_serialized = snapshot_date.isoformat()
        timestamp_token = snapshot_at_bangkok.format("YYYYMMDD_HHmmss")
        s3_key = (
            f"{S3_PREFIX}/year={snapshot_at_bangkok:%Y}/"
            f"month={snapshot_at_bangkok:%m}/day={snapshot_at_bangkok:%d}/"
            f"station_information_{timestamp_token}.json"
        )
        source_object_uri = f"s3://{S3_BUCKET}/{s3_key}"
        staging_table_id = (
            f"{GCP_PROJECT}.{BIGQUERY_DATASET}."
            f"_load_station_information_{timestamp_token}"
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
        # Match station_status replacement's requirement for one snapshot.
        if not stations:
            raise ValueError("data.stations must not be empty")

        rental_uri_unknown: dict[str, Counter[str]] = defaultdict(Counter)

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

            name = station.get("name")
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"{location}.name must be a non-empty string")
            for field, lower, upper in (("lat", -90, 90), ("lon", -180, 180)):
                value = station.get(field)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not lower <= value <= upper
                ):
                    raise ValueError(
                        f"{location}.{field} must be a number in [{lower}, {upper}]"
                    )

            short_name = _optional_string(station, "short_name", location)
            region_id = _optional_string(station, "region_id", location)
            capacity = _optional_integer(station, "capacity", location)
            if capacity is not None and capacity < 0:
                raise ValueError(f"{location}.capacity must be non-negative")
            rental_methods = _string_array(station, "rental_methods", location)
            rental_uris = None
            if "rental_uris" in station:
                source_uris = station["rental_uris"]
                if not isinstance(source_uris, dict):
                    raise ValueError(f"{location}.rental_uris must be an object")
                for field, value in source_uris.items():
                    if field not in RENTAL_URI_FIELDS:
                        rental_uri_unknown[field][_json_type(value)] += 1
                rental_uris = {
                    field: _optional_string(
                        source_uris, field, f"{location}.rental_uris"
                    )
                    for field in sorted(RENTAL_URI_FIELDS)
                }

            extensions = {
                field: _nullable_extension(station, field, expected_type, location)
                for field, expected_type in (
                    ("has_kiosk", bool),
                    ("station_type", str),
                    ("external_id", str),
                    ("electric_bike_surcharge_waiver", bool),
                )
            }

            rows.append(
                {
                    "snapshot_at": snapshot_at_serialized,
                    "snapshot_date": snapshot_date_serialized,
                    "source_object_uri": source_object_uri,
                    "version": version,
                    "last_updated": last_updated,
                    "ttl": ttl,
                    "station_id": station_id,
                    "name": name,
                    "short_name": short_name,
                    "lat": station["lat"],
                    "lon": station["lon"],
                    "region_id": region_id,
                    "capacity": capacity,
                    "rental_methods": rental_methods,
                    "rental_uris": rental_uris,
                    **extensions,
                }
            )

        _warn_for_unknown_fields("root", root_unknown, 1)
        _warn_for_unknown_fields("data", data_unknown, 1)
        _warn_for_unknown_fields("station", station_unknown, len(stations))

        _warn_for_unknown_fields(
            "station.rental_uris", rental_uri_unknown, len(stations)
        )

        client = bigquery.Client(project=GCP_PROJECT)
        job_config = bigquery.LoadJobConfig(
            schema=STAGING_SCHEMA,
            autodetect=False,
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
              COUNTIF(snapshot_at != @snapshot_at) AS other_snapshot_count,
              COUNT(DISTINCT snapshot_at) AS distinct_snapshot_count,
              COUNT(*) - COUNT(DISTINCT CONCAT(
                FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%Ez', snapshot_at),
                '\\x1f',
                station_id
              )) AS duplicate_key_count
            FROM `{staging_table_id}`
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
        if (
            verification.distinct_snapshot_count != 1
            or verification.other_snapshot_count != 0
        ):
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
              name,
              short_name,
              lat,
              lon,
              region_id,
              capacity,
              rental_methods,
              rental_uris,
              has_kiosk,
              station_type,
              external_id,
              electric_bike_surcharge_waiver
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
              name,
              short_name,
              lat,
              lon,
              region_id,
              capacity,
              rental_methods,
              rental_uris,
              has_kiosk,
              station_type,
              external_id,
              electric_bike_surcharge_waiver
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


citibike_station_information_to_bq()
