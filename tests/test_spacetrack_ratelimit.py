import auspex_lakehouse.bronze.dlt.sources.spacetrack._common as c


class FakeClock:
    """Controllable clock whose sleep() advances its own time, so the limiter's
    `while True: sleep(wait)` loop terminates under test (load-bearing contract)."""

    def __init__(self, start=0.0):
        self.t = start

    def now(self):
        return self.t

    def sleep(self, dt):
        self.t += dt


class _AcquireCounter:
    def __init__(self, log):
        self._log = log

    def acquire(self):
        self._log.append("acquire")


def test_under_cap_does_not_sleep():
    clock = FakeClock()
    lim = c._RateLimiter(3, 100, now=clock.now, sleep=clock.sleep)
    lim.acquire()
    lim.acquire()
    assert clock.t == 0.0  # under the per-minute cap -> no wait


def test_per_minute_cap_forces_one_window_wait():
    clock = FakeClock()
    lim = c._RateLimiter(2, 100, now=clock.now, sleep=clock.sleep)
    lim.acquire()  # t=0
    lim.acquire()  # t=0 -> at the per-minute cap of 2
    lim.acquire()  # 3rd within 60s -> sleep until the oldest ages out
    assert clock.t == 60.0


def test_recovers_after_window_passes():
    clock = FakeClock()
    lim = c._RateLimiter(2, 100, now=clock.now, sleep=clock.sleep)
    lim.acquire()
    lim.acquire()
    lim.acquire()           # slept to t=60
    before = clock.t
    lim.acquire()           # room again -> no further sleep
    assert clock.t == before


def test_per_hour_cap_trips_independently():
    clock = FakeClock()
    lim = c._RateLimiter(1000, 2, now=clock.now, sleep=clock.sleep)  # tight per-hour
    lim.acquire()  # t=0
    lim.acquire()  # t=0
    lim.acquire()  # 3rd within the hour -> sleep ~3600s
    assert clock.t == 3600.0


def test_config_caps_are_under_published_ceilings():
    from auspex_lakehouse.bronze.dlt.sources.spacetrack.config import (
        SPACETRACK_MAX_PER_HOUR,
        SPACETRACK_MAX_PER_MIN,
    )

    assert SPACETRACK_MAX_PER_MIN == 25 and SPACETRACK_MAX_PER_MIN < 30
    assert SPACETRACK_MAX_PER_HOUR == 250 and SPACETRACK_MAX_PER_HOUR < 300


def test_throttle_acquires_on_prod(monkeypatch):
    calls = []
    monkeypatch.setattr(c, "_LIMITER", _AcquireCounter(calls))
    monkeypatch.delenv("SPACETRACK_USE_TEST_HOST", raising=False)
    c._throttle()
    assert calls == ["acquire"]


def test_throttle_bypassed_on_test_host(monkeypatch):
    calls = []
    monkeypatch.setattr(c, "_LIMITER", _AcquireCounter(calls))
    monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", "true")
    c._throttle()
    assert calls == []
