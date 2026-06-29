# src/auspex_lakehouse/bronze/dlt/sources/celestrak/config.py
"""CelesTrak provider constants.

CelesTrak SpaceData files are public static CSVs on a CDN — no auth, no real rate
limit. The pool is a convention (matches nasa_api / spacetrack_api) and inherits the
instance-wide default_limit of 1; we do NOT add a named pool to dagster.yaml.
"""

CELESTRAK_API_POOL = "celestrak_api"  # Dagster pool; inherits default_limit=1
