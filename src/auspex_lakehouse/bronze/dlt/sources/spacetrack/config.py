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
# These bound a SINGLE run only and do NOT pace multi-run backfills. Backfill runs are
# auto-routed to the unlimited test host by the asset (the dagster/backfill tag); set
# SPACETRACK_USE_TEST_HOST=true to also force the test host for an ordinary manual run.
SPACETRACK_MAX_PER_MIN = 25
SPACETRACK_MAX_PER_HOUR = 250

# A backfill fans partitions out across separate processes whose in-process limiters
# cannot see one another, so the combined rate can still trip space-track's ceiling and
# earn an HTTP 429. Rather than failing the whole asset, back off and retry: honor the
# response's Retry-After header when present, else wait the default below. Caps the total
# blocking at MAX_RETRIES * the wait, so a sustained 429 eventually surfaces as an error.
SPACETRACK_MAX_RETRIES = 5
SPACETRACK_RETRY_WAIT_DEFAULT_S = 60.0
