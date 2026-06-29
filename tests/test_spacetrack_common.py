from datetime import date
from unittest.mock import Mock

import pytest

import auspex_lakehouse.bronze.dlt.sources.spacetrack._common as c


def test_iter_days_is_inclusive():
    assert list(c.iter_days(date(2026, 1, 1), date(2026, 1, 3))) == [
        date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
    ]


def test_query_class_builds_url_with_segments():
    sess = Mock()
    resp = Mock(raise_for_status=Mock())
    resp.json.return_value = [{"a": 1}]
    sess.get.return_value = resp

    out = c.query_class(sess, "gp", "orderby", "NORAD_CAT_ID")

    assert out == [{"a": 1}]
    assert sess.get.call_args[0][0] == (
        f"{c.BASE_URL}/basicspacedata/query/class/gp/"
        "orderby/NORAD_CAT_ID/format/json"
    )


def test_query_class_builds_url_without_segments():
    sess = Mock()
    resp = Mock(raise_for_status=Mock())
    resp.json.return_value = []
    sess.get.return_value = resp

    c.query_class(sess, "boxscore")

    assert sess.get.call_args[0][0] == (
        f"{c.BASE_URL}/basicspacedata/query/class/boxscore/format/json"
    )


def _fake_requests(probe_resp):
    sess = Mock()
    sess.post.return_value = Mock(status_code=200, raise_for_status=Mock())
    sess.get.return_value = probe_resp
    fake = Mock()
    fake.Session.return_value = sess
    return fake, sess


def test_login_success_returns_session(monkeypatch):
    probe = Mock(status_code=200)
    probe.json.return_value = [{"ok": 1}]
    fake, sess = _fake_requests(probe)
    monkeypatch.setattr(c, "requests", fake)
    monkeypatch.setattr(c, "spacetrack_credentials", lambda: ("user", "pass"))

    result = c.login_session()

    assert result is sess
    assert sess.post.call_args.kwargs["data"] == {"identity": "user", "password": "pass"}


def test_login_failure_raises_on_non_json_probe(monkeypatch):
    probe = Mock(status_code=200)
    probe.json.side_effect = ValueError("not json")
    fake, _ = _fake_requests(probe)
    monkeypatch.setattr(c, "requests", fake)
    monkeypatch.setattr(c, "spacetrack_credentials", lambda: ("user", "pass"))

    with pytest.raises(RuntimeError):
        c.login_session()


def test_use_test_host_default_false(monkeypatch):
    monkeypatch.delenv("SPACETRACK_USE_TEST_HOST", raising=False)
    assert c._use_test_host() is False


def test_use_test_host_truthy_values(monkeypatch):
    for v in ["1", "true", "TRUE", "Yes", " yes "]:
        monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", v)
        assert c._use_test_host() is True, v


def test_use_test_host_non_truthy_values(monkeypatch):
    for v in ["0", "false", "no", "", "off"]:
        monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", v)
        assert c._use_test_host() is False, v


def test_prod_base_url_uses_www_host():
    # space-track's TLS cert is valid for the www. subdomain (and *.space-track.org),
    # NOT the bare apex. Hitting the apex fails the handshake with a hostname mismatch.
    assert c.BASE_URL == "https://www.space-track.org"


def test_base_url_switches_on_toggle(monkeypatch):
    monkeypatch.delenv("SPACETRACK_USE_TEST_HOST", raising=False)
    assert c._base_url() == c.BASE_URL
    monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", "true")
    assert c._base_url() == c.DEV_BASE_URL


@pytest.fixture(autouse=True)
def _fresh_limiter(monkeypatch):
    # Isolate the shared singleton AND guarantee no real time.sleep: a never-tripping
    # limiter for every test in this file that exercises the real _throttle path.
    monkeypatch.setattr(c, "_LIMITER", c._RateLimiter(10**9, 10**9))


def test_query_class_calls_throttle(monkeypatch):
    calls = []
    monkeypatch.setattr(c, "_throttle", lambda: calls.append(1))
    sess = Mock()
    resp = Mock(raise_for_status=Mock())
    resp.json.return_value = []
    sess.get.return_value = resp
    c.query_class(sess, "boxscore")
    assert calls == [1]


def test_login_session_calls_throttle_for_post_and_probe(monkeypatch):
    calls = []
    monkeypatch.setattr(c, "_throttle", lambda: calls.append(1))
    probe = Mock(status_code=200)
    probe.json.return_value = [{"ok": 1}]
    fake, _ = _fake_requests(probe)
    monkeypatch.setattr(c, "requests", fake)
    monkeypatch.setattr(c, "spacetrack_credentials", lambda: ("user", "pass"))
    c.login_session()
    assert calls == [1, 1]  # one before the POST, one before the probe GET


def test_query_class_uses_test_host_url_when_toggled(monkeypatch):
    monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", "true")
    sess = Mock()
    resp = Mock(raise_for_status=Mock())
    resp.json.return_value = []
    sess.get.return_value = resp
    c.query_class(sess, "boxscore")
    assert sess.get.call_args[0][0].startswith(c.DEV_BASE_URL)
