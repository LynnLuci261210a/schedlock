"""Unit tests for TimeoutBackend."""
from __future__ import annotations

import threading
import time

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.timeout_backend import TimeoutBackend


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def inner():
    return MemoryBackend()


@pytest.fixture()
def backend(inner):
    return TimeoutBackend(inner, timeout_seconds=2.0)


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        TimeoutBackend("not-a-backend")  # type: ignore[arg-type]


def test_requires_positive_timeout(inner):
    with pytest.raises(ValueError):
        TimeoutBackend(inner, timeout_seconds=0)


def test_requires_numeric_timeout(inner):
    with pytest.raises(ValueError):
        TimeoutBackend(inner, timeout_seconds="fast")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_timeout_seconds_property(backend):
    assert backend.timeout_seconds == 2.0


# ---------------------------------------------------------------------------
# Acquire behaviour
# ---------------------------------------------------------------------------


def test_acquire_succeeds_when_inner_fast(backend):
    assert backend.acquire("job", "w1", 60) is True


def test_acquire_fails_when_already_locked(backend):
    backend.acquire("job", "w1", 60)
    assert backend.acquire("job", "w2", 60) is False


def test_acquire_times_out_when_inner_blocks(inner):
    """If inner.acquire blocks longer than timeout, False is returned promptly."""
    barrier = threading.Event()

    original_acquire = inner.acquire

    def slow_acquire(key, owner, ttl):
        barrier.wait(timeout=10)  # blocks until test releases it
        return original_acquire(key, owner, ttl)

    inner.acquire = slow_acquire  # type: ignore[method-assign]

    tb = TimeoutBackend(inner, timeout_seconds=0.1)
    start = time.monotonic()
    result = tb.acquire("job", "w1", 60)
    elapsed = time.monotonic() - start

    barrier.set()  # unblock background thread so it can exit cleanly
    assert result is False
    assert elapsed < 1.0  # should have returned well within 1 s


# ---------------------------------------------------------------------------
# Delegate methods
# ---------------------------------------------------------------------------


def test_release_delegates(inner, backend):
    backend.acquire("job", "w1", 60)
    assert backend.release("job", "w1") is True
    assert inner.is_locked("job") is False


def test_is_locked_delegates(inner, backend):
    assert backend.is_locked("job") is False
    inner.acquire("job", "w1", 60)
    assert backend.is_locked("job") is True


def test_refresh_delegates(inner, backend):
    backend.acquire("job", "w1", 60)
    assert backend.refresh("job", "w1", 120) is True
