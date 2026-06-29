# src/auspex_lakehouse/bronze/dlt/sources/celestrak/_common.py
import io

import polars as pl
import requests

SW_ALL_URL = "https://celestrak.org/SpaceData/SW-All.csv"


def fetch_csv_rows(url: str) -> list[dict]:
    """GET a CelesTrak SpaceData CSV and return typed row dicts.

    `infer_schema_length=None` scans the WHOLE file before typing — required here:
    the F10.7 81-day-average columns are blank for the first ~80 rows (Oct 1957,
    before a full window exists), and prediction rows carry blanks too. Under the
    default 100-row inference those all-null-early columns mis-type (or raise) when
    real values appear later. Full-file inference is cheap (~25k rows). Blank fields
    map to null; numeric columns type as numeric; DATE and F10.7_DATA_TYPE stay
    strings (bronze keeps DATE raw — silver casts to a date).
    """
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    df = pl.read_csv(io.BytesIO(resp.content), infer_schema_length=None)
    return df.to_dicts()
