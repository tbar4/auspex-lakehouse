from collections.abc import Iterator
from datetime import date, timedelta

import dlt
import requests  # stdlib requests: cookie-session persistence across queries

BASE_URL = "https://for-testing-only.space-track.org"


def spacetrack_credentials() -> tuple[str, str]:
    """(username, password) from dlt secrets (env SPACETRACK_USERNAME / _PASSWORD)."""
    return dlt.secrets["spacetrack_username"], dlt.secrets["spacetrack_password"]


def _looks_like_json_payload(resp) -> bool:
    """True if the body parses as JSON (not an HTML login redirect)."""
    try:
        resp.json()
        return True
    except ValueError:
        return False


def login_session() -> requests.Session:
    """Authenticate and return a cookie-bearing session.

    space-track may return HTTP 200 even on bad credentials, and the success/failure
    body is unspecified, so we verify auth with one trivial authenticated probe rather
    than matching a body string. An unauthenticated session redirects to the login page
    (non-JSON body) instead of returning a JSON list.
    """
    username, password = spacetrack_credentials()
    session = requests.Session()
    resp = session.post(
        f"{BASE_URL}/ajaxauth/login",
        data={"identity": username, "password": password},
        timeout=60,
    )
    resp.raise_for_status()
    probe = session.get(
        f"{BASE_URL}/basicspacedata/query/class/boxscore/limit/1/format/json",
        timeout=60,
    )
    if probe.status_code != 200 or not _looks_like_json_payload(probe):
        raise RuntimeError(
            "space-track login failed (check SPACETRACK_USERNAME / SPACETRACK_PASSWORD)"
        )
    return session


def query_class(session: requests.Session, cls: str, *segments: str):
    """GET /basicspacedata/query/class/<cls>/<segments>/format/json -> parsed JSON."""
    path = "/".join(segments)
    sep = "/" if path else ""
    url = f"{BASE_URL}/basicspacedata/query/class/{cls}{sep}{path}/format/json"
    resp = session.get(url, timeout=120)
    resp.raise_for_status()
    return resp.json()


def iter_days(start_date: date, end_date: date) -> Iterator[date]:
    """Yield each date in the inclusive [start_date, end_date] range."""
    day = start_date
    while day <= end_date:
        yield day
        day += timedelta(days=1)
