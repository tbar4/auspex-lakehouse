from datetime import date

import dlt

from auspex_lakehouse.bronze.dlt.sources.nasa.apod import apod
from auspex_lakehouse.bronze.dlt.sources.nasa.neows import neows


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

__all__ = ["apod", "neows", "nasa_api", "nasa_pipeline"]
