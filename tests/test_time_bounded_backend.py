"""Tests for TimeBoundedBackend."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, call

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.time_bounded_backend import TimeBoundedBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return TimeBoundedBackend(inner, max_hold_seconds=5.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        TimeBoundedBackend("not-a-backend", max_hold_seconds=5.0)


def test_requires_positive_max_hold(inner):
    with pytest.raises(ValueError, match="max_hold_seconds must be a positive number"):
        TimeBoundedBackend(inner, max_hold_seconds=0)


def test_requires_numeric_max_hold(inner):
    with pytest.raises(ValueError):
        TimeBoundedBackend(inner, max_hold_seconds="5")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_hold_seconds_property(inner):
    b = TimeBoundedBackend(inner, max_hold_seconds=10)
    assert b.max_hold_seconds == 10.0


def test_acquire_delegates_to_inner(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", owner="worker-1", ttl=30)
    assert result is True
    inner.acquire.assert_called_once_with("job", owner="worker-1", ttl=30)


def test_acquire_records_timestamp_on_success(inner, backend):
    inner.acquire.return_value = True
    before = time.monotonic()
    backend.acquire("job", owner="worker-1", ttl=30)
    after = time.monotonic()
    ts = backend._acquired_at.get("job")
    assert ts is not None
    assert before <= ts <= after


def test_acquire_does_not_record_timestamp_on_failure(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", owner="worker-1", ttl=30)
    assert "job" not in backend._acquired_at


def test_release_delegates_to_inner(inner, backend):
    inner.release.return_value = True
    result = backend.release("job", owner="worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", owner="worker-1")


def test_release_clears_timestamp(inner, backend):
    inner.acquire.return_value = True
    inner.release.return_value = True
    backend.acquire("job", owner="worker-1", ttl=30)
    backend.release("job", owner="worker-1")
    assert "job" not in backend._acquired_at


def test_is_locked_delegates_to_inner(inner, backend):
    inner.is_locked.return_value = (True, "worker-1")
    result = backend.is_locked("job")
    assert result == (True, "worker-1")


def test_overdue_lock_evicted_on_acquire(inner):
    b = TimeBoundedBackend(inner, max_hold_seconds=0.05)
    inner.acquire.return_value = True
    inner.is_locked.return_value = (True, "worker-1")
    inner.release.return_value = True
    # First acquire
    b.acquire("job", owner="worker-1", ttl=30)
    # Simulate time passing beyond max hold
    b._acquired_at["job"] = time.monotonic() - 1.0
    # Second acquire should evict first
    b.acquire("job", owner="worker-2", ttl=30)
    # release should have been called to evict
    inner.release.assert_called()


def test_overdue_lock_evicted_on_is_locked(inner):
    b = TimeBoundedBackend(inner, max_hold_seconds=0.05)
    inner.acquire.return_value = True
    inner.is_locked.return_value = (True, "worker-1")
    inner.release.return_value = True
    b.acquire("job", owner="worker-1", ttl=30)
    b._acquired_at["job"] = time.monotonic() - 1.0
    b.is_locked("job")
    inner.release.assert_called()


def test_refresh_delegates_to_inner(inner, backend):
    inner.refresh.return_value = True
    result = backend.refresh("job", owner="worker-1", ttl=60)
    assert result is True
    inner.refresh.assert_called_once_with("job", owner="worker-1", ttl=60)
