# src/auspex_lakehouse/bronze/dlt/sources/spacetrack/incremental.py
from datetime import timedelta

import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import iter_days, query_class


def _incremental_resource(name, cls, primary_key, date_predicate):
    @dlt.resource(
        name=name,
        write_disposition="merge",
        primary_key=primary_key,
        table_format="delta",
    )
    def _resource(session, start_date, end_date):
        for day in iter_days(start_date, end_date):
            window = f"{day.isoformat()}--{(day + timedelta(days=1)).isoformat()}"
            data = query_class(session, cls, date_predicate, window)
            if isinstance(data, list):
                yield from data

    return _resource


INCREMENTAL_CLASSES = [
    # (name, class, primary_key, date_predicate)
    ("space_track_decays",                          "decay",      ["NORAD_CAT_ID", "MSG_EPOCH", "PRECEDENCE"], "MSG_EPOCH"),
    ("space_track_conjunction_data_messages",       "cdm_public", "CDM_ID",                                    "CREATED"),
    ("space_track_tracking_and_impact_predictions", "tip",        ["NORAD_CAT_ID", "MSG_EPOCH"],               "INSERT_EPOCH"),
]
