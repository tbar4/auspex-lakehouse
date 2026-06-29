import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import query_class


def _snapshot_resource(name, cls, primary_key, segments, write_disposition, min_rows):
    @dlt.resource(
        name=name,
        write_disposition=write_disposition,
        primary_key=primary_key,        # None for replace tables
        table_format="delta",
    )
    def _resource(session):
        data = query_class(session, cls, *segments)
        if not isinstance(data, list):  # tolerate empty/non-list bodies
            return
        if min_rows and len(data) < min_rows:
            # Suspected truncation / implicit row-cap — fail loudly rather than
            # silently writing a short catalog. Floor is conservative.
            raise RuntimeError(
                f"{name}: {len(data)} rows < floor {min_rows}; suspected row-cap"
            )
        yield from data

    return _resource


SNAPSHOT_CLASSES = [
    # (name, class, primary_key, segments, write_disposition, min_rows)
    ("gp",       "gp",       "NORAD_CAT_ID",
     ("orderby", "NORAD_CAT_ID"),                         "merge",   10000),
    ("satcat",   "satcat",   "NORAD_CAT_ID",
     ("CURRENT", "Y", "orderby", "NORAD_CAT_ID"),         "merge",   10000),
    ("boxscore", "boxscore", None,
     (),                                                   "replace", None),
]
