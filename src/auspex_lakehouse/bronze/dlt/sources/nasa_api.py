from datetime import date, timedelta

import dlt
from dlt.sources.helpers import requests

BASE_URL = "https://api.nasa.gov"


def _iter_days(start_date: date, end_date: date):
    """Yield each date in the inclusive [start_date, end_date] range."""
    day = start_date
    while day <= end_date:
        yield day
        day += timedelta(days=1)


@dlt.resource(name="apod", write_disposition="merge", primary_key="date", table_format="delta")
def apod(start_date: date, end_date: date):
    for day in _iter_days(start_date, end_date):
        resp = requests.get(
            f"{BASE_URL}/planetary/apod",
            params={
                "api_key": dlt.secrets["nasa_api_key"],
                "date": day.isoformat(),
            },
        )
        resp.raise_for_status()
        yield resp.json()

@dlt.resource(
    name="neows",
    write_disposition="merge",
    primary_key=["date", "id"],
    table_format="delta",
)
def neows(start_date: date, end_date: date):
    for day in _iter_days(start_date, end_date):
        resp = requests.get(
            f"{BASE_URL}/neo/rest/v1/feed",
            params={
                "api_key": dlt.secrets["nasa_api_key"],
                "start_date": day.isoformat(),
                "end_date": day.isoformat(),
            },
        )
        resp.raise_for_status()
        # The feed nests asteroids under near_earth_objects keyed by date.
        # Flatten to one row per asteroid, tagged with its feed date.
        for feed_date, objects in resp.json()["near_earth_objects"].items():
            for obj in objects:
                yield {**obj, "date": feed_date}

@dlt.source
def nasa_api(start_date: date, end_date: date):
    return [
        apod(start_date, end_date),
        neows(start_date, end_date),
    ]


nasa_pipeline = dlt.pipeline(
    pipeline_name="nasa_api",
    destination="filesystem",
    dataset_name="bronze",
)

