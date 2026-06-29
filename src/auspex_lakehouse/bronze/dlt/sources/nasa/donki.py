from datetime import date

import dlt
from dlt.sources.helpers import requests

from auspex_lakehouse.bronze.dlt.sources.nasa._common import BASE_URL, iter_days, nasa_api_key


def _donki_resource(name, endpoint_path, primary_key, extra_params=None):
    """Build a merge-on-ID dlt resource for one DONKI endpoint. Bulk list query
    per partition-day; tolerates empty/non-list bodies without writing junk."""

    @dlt.resource(
        name=name, write_disposition="merge", primary_key=primary_key, table_format="delta"
    )
    def _resource(start_date: date, end_date: date):
        api_key = nasa_api_key()
        for day in iter_days(start_date, end_date):
            params = {
                "api_key": api_key,
                "startDate": day.isoformat(),
                "endDate": day.isoformat(),
                **(extra_params or {}),
            }
            resp = requests.get(f"{BASE_URL}/DONKI/{endpoint_path}", params=params)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                yield from data

    return _resource


# (resource_name, endpoint_path, primary_key, extra_params)
DONKI_ENDPOINTS = [
    ("cme",                   "CME",                 "activityID",                     None),
    ("cme_analysis",          "CMEAnalysis",         ["associatedCMEID", "time21_5"],  None),
    ("gst",                   "GST",                 "gstID",                          None),
    ("ips",                   "IPS",                 "activityID",                     None),
    ("flr",                   "FLR",                 "flrID",                          None),
    ("sep",                   "SEP",                 "sepID",                          None),
    ("mpc",                   "MPC",                 "mpcID",                          None),
    ("rbe",                   "RBE",                 "rbeID",                          None),
    ("hss",                   "HSS",                 "hssID",                          None),
    ("wsa_enlil_simulations", "WSAEnlilSimulations", "simulationID",                   None),
    ("notifications", "notifications", "messageID", {"type": "all"}),
]


# name="nasa_donki" → dagster-dlt asset keys become dlt_nasa_donki_<resource>
# (the export stays donki_source).
@dlt.source(name="nasa_donki")
def donki_source(start_date: date, end_date: date):
    return [
        _donki_resource(name, path, pk, extra)(start_date, end_date)
        for (name, path, pk, extra) in DONKI_ENDPOINTS
    ]


nasa_donki_pipeline = dlt.pipeline(
    pipeline_name="nasa_donki",  # distinct working dir; no collision with nasa_api
    destination="filesystem",
    dataset_name="bronze",
)
