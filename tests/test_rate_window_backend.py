"""Tests for RateWindowBackend."""

import time
import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.rate_window_backend import RateWindowBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture
def backend(inner):
    return RateWindowBackend(inner, max_acquires=3, window=60.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        RateWindowBackend("not-a-backend", max_acquires=2, window=10.0)


def test_requires_positive_max_acquires(inner):
    with pytest.raises(ValueError):
        RateWindowBackend(inner, max_acquires=0, window=10.0)


def test_requires_positive_window(inner):
    with pytest.raises(ValueError):
        RateWindowBackend(inner, max_acquires=2, window=0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_acquires_property(backend):
    assert backend.max_acquires == 3


def test_window_property(backend):
    assert backend.window == 60.0


def test_acquire_succeeds_within_limit(inner, backend):
    assert backend.acquire("job", "owner-1", 30) is True
    inner.acquire.assert_called_once_with("job", "owner-1", 30)


def test_acquire_blocked_when_limit_reached(inner):
    b = RateWindowBackend(inner, max_acquires=2, window=60.0)
    b.acquire("job", "owner-1", 30)
    b.acquire("job", "owner-2", 30)
    result = b.acquire("job", "owner-3", 30)
    assert result is False
    assert inner.acquire.call_count == 2


def test_acquire_does_not_count_failed_inner(inner):
    inner.acquire.return_value = False
    b = RateWindowBackend(inner, max_acquires=2, window=60.0)
    b.acquire("job", "owner-1", 30)
    b.acquire("job", "owner-2", 30)
    # Neither counted, so a third attempt still reaches inner
    assert inner.acquire.call_count == 3


def test_keys_are_tracked_independently(inner):
    b = RateWindowBackend(inner, max_acquires=1, window=60.0)
    assert b.acquire("job-a", "owner", 30) is True
    assert b.acquire("job-b", "owner", 30) is True


def test_count_for_returns_current_window_count(inner):
    b = RateWindowBackend(inner, max_acquires=5, window=60.0)
    assert b.count_for("job") == 0
    b.acquire("job", "owner-1", 30)
    b.acquire("job", "owner-2", 30)
    assert b.count_for("job") == 2


def test_timestamps_pruned_after_window(inner):
    b = RateWindowBackend(inner, max_acquires=2, window=0.1)
    b.acquire("job", "owner-1", 30)
    b.acquire("job", "owner-2", 30)
    time.sleep(0.15)
    # Window expired — should allow new acquires
    assert b.acquire("job", "owner-3", 30) is True


def test_release_delegates(inner, backend):
    backend.release("job", "owner-1")
    inner.release.assert_called_once_with("job", "owner-1")


def test_is_locked_delegates(inner, backend):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    backend.refresh("job", "owner-1", 60)
    inner.refresh.assert_called_once_with("job", "owner-1", 60)
