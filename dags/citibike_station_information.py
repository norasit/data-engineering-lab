import json
import logging
from urllib.request import Request, urlopen

import boto3
import pendulum
from airflow.sdk import dag, get_current_context, task


STATION_INFORMATION_URL = (
    "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
)
S3_BUCKET = "data-engineering-lab-nk"
S3_PREFIX = "raw/citibike/station_information"
BANGKOK_TIMEZONE = pendulum.timezone("Asia/Bangkok")
LOGGER = logging.getLogger(__name__)


@dag(
    dag_id="citibike_station_information_poll",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 8, 28, 6, 0, tz=BANGKOK_TIMEZONE),
    catchup=False,
)
def citibike_station_information_poll():
    @task(retries=1, do_xcom_push=False)
    def fetch_station_information():
        logical_date = get_current_context().get("logical_date")
        if logical_date is None:
            raise ValueError("DAG run must have a logical date")

        scheduled_at_bangkok = pendulum.instance(logical_date).in_timezone(
            BANGKOK_TIMEZONE
        )
        s3_key = (
            f"{S3_PREFIX}/"
            f"year={scheduled_at_bangkok:%Y}/"
            f"month={scheduled_at_bangkok:%m}/"
            f"day={scheduled_at_bangkok:%d}/"
            f"station_information_{scheduled_at_bangkok:%Y%m%d_%H%M%S}.json"
        )

        request = Request(
            STATION_INFORMATION_URL,
            headers={"User-Agent": "citibike-data-engineering-lab/1.0"},
        )

        with urlopen(request, timeout=30) as response:
            if response.status != 200:
                raise RuntimeError(
                    f"Citi Bike returned unexpected HTTP status {response.status}"
                )

            content_type = response.headers.get_content_type()
            if content_type != "application/json":
                raise RuntimeError(
                    f"Citi Bike returned unexpected content type {content_type!r}"
                )

            response_body = response.read()
            payload = json.loads(response_body)

        if not isinstance(payload, dict):
            raise ValueError("GBFS response must be a JSON object")
        if payload.get("version") != "1.1":
            raise ValueError("GBFS response must declare version 1.1")

        last_updated = payload.get("last_updated")
        if not isinstance(last_updated, int):
            raise ValueError("GBFS last_updated must be an integer")
        if not isinstance(payload.get("ttl"), int):
            raise ValueError("GBFS ttl must be an integer")

        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("GBFS data must be a JSON object")

        stations = data.get("stations")
        if not isinstance(stations, list):
            raise ValueError("GBFS data.stations must be a list")

        station_ids = []
        for station in stations:
            if not isinstance(station, dict):
                raise ValueError("Every GBFS station must be a JSON object")

            station_id = station.get("station_id")
            if not isinstance(station_id, str) or not station_id:
                raise ValueError("Every GBFS station must have a string station_id")

            name = station.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError("Every GBFS station must have a string name")

            latitude = station.get("lat")
            if (
                isinstance(latitude, bool)
                or not isinstance(latitude, (int, float))
                or not -90 <= latitude <= 90
            ):
                raise ValueError("Every GBFS station must have a valid latitude")

            longitude = station.get("lon")
            if (
                isinstance(longitude, bool)
                or not isinstance(longitude, (int, float))
                or not -180 <= longitude <= 180
            ):
                raise ValueError("Every GBFS station must have a valid longitude")

            capacity = station.get("capacity")
            if capacity is not None and (
                isinstance(capacity, bool)
                or not isinstance(capacity, int)
                or capacity < 0
            ):
                raise ValueError("GBFS station capacity must be a non-negative integer")

            station_ids.append(station_id)

        boto3.client("s3").put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=response_body,
            ContentType="application/json",
        )

        LOGGER.info(
            "feed last_updated=%s station_count=%s sample_station_ids=%s",
            last_updated,
            len(stations),
            station_ids[:5],
        )

    fetch_station_information()


citibike_station_information_poll()
