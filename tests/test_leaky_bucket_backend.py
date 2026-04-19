"""Tests for LeakyBucketBackend."""

import time
import pytest
from unittest.mock import MagicMock
from schedlock.backends.leaky_bucket_backend import LeakyBucketBackend
from schedlock.backends.base import BaseBackend


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
    return LeakyBucketBackend(inner, rate=3, window=60.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        LeakyBucketBackend("not-a-backend")


def test_requires_positive_rate(inner):
    with pytest.raises(ValueError):
        LeakyBucketBackend(inner, rate=0)


def test_requires_positive_window(inner):
    with pytest.raises(ValueError):
        LeakyBucketBackend(inner, rate=1, window=0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_rate_property(inner, backend):
    assert backend.rate == 3


def test_window_property(inner, backend):
    assert backend.window == 60.0


def test_acquire_within_rate_succeeds(inner, backend):
    for _ in range(3):
        assert backend.acquire("job", "owner", 30) is True
    assert inner.acquire.call_count == 3


def test_acquire_blocked_after_rate_exceeded(inner, backend):
    for _ in range(3):
        backend.acquire("job", "owner", 30)
    result = backend.acquire("job", "owner", 30)
    assert result is False
    assert inner.acquire.call_count == 3


def test_acquire_not_counted_when_inner_fails(inner, backend):
    inner.acquire.return_value = False
    for _ in range(5):
        assert backend.acquire("job", "owner", 30) is False
    # bucket should still be empty — all failed
    inner.acquire.return_value = True
    assert backend.acquire("job", "owner", 30) is True


def test_release_delegates(inner, backend):
    backend.release("job", "owner")
    inner.release.assert_called_once_with("job", "owner")


def test_is_locked_delegates(inner, backend):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    backend.refresh("job", "owner", 30)
    inner.refresh.assert_called_once_with("job", "owner", 30)


def test_window_expiry_allows_reacquire(inner):
    backend = LeakyBucketBackend(inner, rate=2, window=0.1)
    backend.acquire("job", "owner", 30)
    backend.acquire("job", "owner", 30)
    assert backend.acquire("job", "owner", 30) is False
    time.sleep(0.15)
    assert backend.acquire("job", "owner", 30) is True
