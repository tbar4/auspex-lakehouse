"""NASA-provider budget constants.

The 1000 calls/hour limit is shared across all NASA endpoints, so these are
deliberately conservative. Other providers get their own constants.
"""

NASA_REFRESH_DAYS = 30           # re-fetch a NEO whose lookup is older than this
NASA_MAX_LOOKUPS_PER_RUN = 500   # secondary per-run guard (primary: pool + 429-handling)
NASA_API_POOL = "nasa_api"       # Dagster concurrency pool serializing NASA API access
