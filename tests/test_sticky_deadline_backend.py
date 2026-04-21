"""Tests for StickyDeadlineBackend."""

import time
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.sticky_deadline_backend import StickyDeadlineBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return StickyDeadlineBackend(inner, deadline_seconds=10.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        StickyDeadlineBackend(object(), deadline_seconds=5.0)


def test_requires_numeric_deadline(inner):
    with pytest.raises(TypeError, match="deadline_seconds must be a number"):
        StickyDeadlineBackend(inner, deadline_seconds="5")


def test_requires_positive_deadline(inner):
    with pytest.raises(ValueError, match="deadline_seconds must be positive"):
        StickyDeadlineBackend(inner, deadline_seconds=0)


def test_requires_positive_deadline_negative(inner):
    with pytest.raises(ValueError, match="deadline_seconds must be positive"):
        StickyDeadlineBackend(inner, deadline_seconds=-1.0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_deadline_seconds_property(inner, backend):
    assert backend.deadline_seconds == 10.0


def test_acquire_succeeds_before_deadline(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_blocked_after_deadline(inner):
    b = StickyDeadlineBackend(inner, deadline_seconds=0.05)
    inner.acquire.return_value = True
    b.acquire("job", "worker-1", 30)
    time.sleep(0.1)
    result = b.acquire("job", "worker-1", 30)
    assert result is False
    assert inner.acquire.call_count == 1


def test_different_owners_tracked_independently(inner):
    b = StickyDeadlineBackend(inner, deadline_seconds=0.05)
    inner.acquire.return_value = True
    b.acquire("job", "worker-A", 30)
    time.sleep(0.1)
    # worker-A is past deadline, worker-B is fresh
    result_a = b.acquire("job", "worker-A", 30)
    result_b = b.acquire("job", "worker-B", 30)
    assert result_a is False
    assert result_b is True


def test_reset_owner_allows_reacquire(inner):
    b = StickyDeadlineBackend(inner, deadline_seconds=0.05)
    inner.acquire.return_value = True
    b.acquire("job", "worker-1", 30)
    time.sleep(0.1)
    b.reset_owner("worker-1")
    result = b.acquire("job", "worker-1", 30)
    assert result is True


def test_release_delegates(inner, backend):
    inner.release.return_value = True
    assert backend.release("job", "worker-1") is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = False
    assert backend.is_locked("job") is False
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    assert backend.refresh("job", "worker-1", 60) is True
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_integration_with_memory_backend():
    memory = MemoryBackend()
    b = StickyDeadlineBackend(memory, deadline_seconds=0.05)
    assert b.acquire("job", "worker-1", 30) is True
    assert b.is_locked("job") is True
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False
    time.sleep(0.1)
    assert b.acquire("job", "worker-1", 30) is False
