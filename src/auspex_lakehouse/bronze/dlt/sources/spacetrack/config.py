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

# Hard request-rate caps enforced in-process by the _RateLimiter in _common.py.
# Login POST + auth probe + each query GET are ALL counted. Set conservatively under
# space-track's published ceilings (<30/min, <300/hr) — plain headroom for clock jitter.
# These bound a SINGLE run only; to backfill, flip SPACETRACK_USE_TEST_HOST=true and use
# the unlimited test host (the throttle does NOT pace multi-run backfills — see design).
SPACETRACK_MAX_PER_MIN = 25
SPACETRACK_MAX_PER_HOUR = 250
