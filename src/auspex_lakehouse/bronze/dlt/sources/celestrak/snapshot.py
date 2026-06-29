# src/auspex_lakehouse/bronze/dlt/sources/celestrak/snapshot.py
import dlt

from auspex_lakehouse.bronze.dlt.sources.celestrak._common import SW_ALL_URL, fetch_csv_rows


def _csv_snapshot_resource(name, url, primary_key, min_rows):
    @dlt.resource(
        name=name,
        write_disposition="merge",
        primary_key=primary_key,
        table_format="delta",
    )
    def _resource():
        rows = fetch_csv_rows(url)
        if min_rows and len(rows) < min_rows:
            # Suspected truncated/short download — fail loudly rather than write a
            # gap-riddled drag-driver table. Floor is conservative (file only grows).
            raise RuntimeError(
                f"{name}: {len(rows)} rows < floor {min_rows}; suspected truncation"
            )
        yield from rows

    return _resource


CELESTRAK_DATASETS = [
    # (name, url, primary_key, min_rows)   name == physical bronze table name
    ("celestrak_space_weather", SW_ALL_URL, "DATE", 20000),
    # EOP slots in here later:
    # ("celestrak_earth_orientation_parameters", EOP_ALL_URL, "DATE", 15000),
]
