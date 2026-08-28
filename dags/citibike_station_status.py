import json
import logging
from urllib.request import Request, urlopen

import boto3
import pendulum
from airflow.sdk import dag, get_current_context, task


STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
S3_BUCKET = "data-engineering-lab-nk"
S3_PREFIX = "raw/citibike/station_status"
BANGKOK_TIMEZONE = pendulum.timezone("Asia/Bangkok")
LOGGER = logging.getLogger(__name__)


@dag(
    dag_id="citibike_station_status_poll",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 8, 28, tz=BANGKOK_TIMEZONE),
    catchup=False,
)
def citibike_station_status_poll():
    @task(retries=1, do_xcom_push=False)
    def fetch_station_status():
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
            f"station_status_{scheduled_at_bangkok:%Y%m%d_%H%M%S}.json"
        )

        request = Request(
            STATION_STATUS_URL,
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

    fetch_station_status()


citibike_station_status_poll()
