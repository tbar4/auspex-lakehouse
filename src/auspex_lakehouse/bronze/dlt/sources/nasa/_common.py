from collections.abc import Iterator
from datetime import date, timedelta

import dlt

BASE_URL = "https://api.nasa.gov"


def iter_days(start_date: date, end_date: date) -> Iterator[date]:
    """Yield each date in the inclusive [start_date, end_date] range."""
    day = start_date
    while day <= end_date:
        yield day
        day += timedelta(days=1)


def nasa_api_key() -> str:
    """NASA API key from dlt config (env NASA_API_KEY or .dlt/secrets.toml)."""
    return dlt.secrets["nasa_api_key"]
