# src/auspex_lakehouse/bronze/dlt/sources/spacetrack/config.py
"""space-track provider constants.

Rate limits (per class) enforced operationally via scheduling + the pool:
    GP        1 / hour       (randomize the minute)
    SATCAT    1 / day        (updated after 1700 UTC)
    BOXSCORE  1 / day
    DECAY     1 / day
    CDM       3 / day        (or 1 / hour for a specific event)
    TIP       1 / hour
    Overall   < 30 / minute, < 300 / hour
All space-track crons run after 1700 UTC so SATCAT reflects the day's update.
"""

SPACETRACK_API_POOL = "spacetrack_api"  # Dagster pool serializing space-track API access
