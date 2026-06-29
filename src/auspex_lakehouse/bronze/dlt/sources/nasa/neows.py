from datetime import date

import dlt
from dlt.sources.helpers import requests

from auspex_lakehouse.bronze.dlt.sources.nasa._common import BASE_URL, iter_days, nasa_api_key


@dlt.resource(
    name="neows",
    write_disposition="merge",
    primary_key=["date", "id"],
    table_format="delta",
)
def neows(start_date: date, end_date: date):
    api_key = nasa_api_key()
    for day in iter_days(start_date, end_date):
        resp = requests.get(
            f"{BASE_URL}/neo/rest/v1/feed",
            params={
                "api_key": api_key,
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
