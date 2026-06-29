from datetime import date

import dlt
from dlt.sources.helpers import requests

from auspex_lakehouse.bronze.dlt.sources.nasa._common import BASE_URL, iter_days, nasa_api_key


@dlt.resource(name="apod", write_disposition="merge", primary_key="date", table_format="delta")
def apod(start_date: date, end_date: date):
    api_key = nasa_api_key()
    for day in iter_days(start_date, end_date):
        resp = requests.get(
            f"{BASE_URL}/planetary/apod",
            params={"api_key": api_key, "date": day.isoformat()},
        )
        resp.raise_for_status()
        yield resp.json()
