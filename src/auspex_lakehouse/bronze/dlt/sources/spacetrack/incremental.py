# src/auspex_lakehouse/bronze/dlt/sources/spacetrack/incremental.py
import logging
from datetime import timedelta

import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import iter_days, query_class

logger = logging.getLogger(__name__)


def _has_full_key(row, pk_fields):
    """True iff every primary-key column is present and non-blank.

    A merge resource needs a usable key on every row: dlt builds the row id by
    hashing the primary-key subset (delta `table_format` uses key_hash), so a
    missing column raises KeyError in normalize and a null one is rejected at
    load. Space-Track classes such as TIP can emit reentry predictions for
    uncatalogued objects with a null/absent NORAD_CAT_ID — those rows can't be
    merged on the chosen key, so we drop them.
    """
    for k in pk_fields:
        v = row.get(k)
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return False
    return True


def _incremental_resource(name, cls, primary_key, date_predicate):
    pk_fields = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    @dlt.resource(
        name=name,
        write_disposition="merge",
        primary_key=primary_key,
        table_format="delta",
    )
    def _resource(session, start_date, end_date):
        dropped = 0
        for day in iter_days(start_date, end_date):
            window = f"{day.isoformat()}--{(day + timedelta(days=1)).isoformat()}"
            data = query_class(session, cls, date_predicate, window)
            if isinstance(data, list):
                for row in data:
                    if _has_full_key(row, pk_fields):
                        yield row
                    else:
                        dropped += 1
        if dropped:
            logger.warning(
                "space-track %s: dropped %d record(s) missing primary-key field(s) %s",
                cls, dropped, pk_fields,
            )

    return _resource


INCREMENTAL_CLASSES = [
    # (name, class, primary_key, date_predicate)
    ("space_track_decays", "decay", ["NORAD_CAT_ID", "MSG_EPOCH", "PRECEDENCE"], "MSG_EPOCH"),
    ("space_track_conjunction_data_messages", "cdm_public", "CDM_ID", "CREATED"),
    ("space_track_tracking_and_impact_predictions", "tip",
     ["NORAD_CAT_ID", "MSG_EPOCH"], "INSERT_EPOCH"),
]
