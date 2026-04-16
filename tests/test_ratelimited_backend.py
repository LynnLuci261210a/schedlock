"""Tests for RateLimitedBackend."""
import pytest
from unittest.mock import MagicMock
from schedlock.ratelimit import RateLimiter
from schedlock.backends.ratelimited_backend import RateLimitedBackend


@pytest.fixture
def inner():
    m = MagicMock()
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture
def backend(inner):
    limiter = RateLimiter(max_attempts=2, window=10)
    return RateLimitedBackend(inner, limiter)


def test_acquire_succeeds_within_limit(backend, inner):
    assert backend.acquire("job", "owner1", 30) is True
    inner.acquire.assert_called_once_with("job", "owner1", 30)


def test_acquire_blocked_after_limit(inner):
    limiter = RateLimiter(max_attempts=1, window=10)
    b = RateLimitedBackend(inner, limiter)
    assert b.acquire("job", "owner1", 30) is True
    assert b.acquire("job", "owner1", 30) is False
    inner.acquire.assert_called_once()


def test_release_delegates(backend, inner):
    backend.release("job", "owner1")
    inner.release.assert_called_once_with("job", "owner1")


def test_is_locked_delegates(backend, inner):
    result = backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")
    assert result is False


def test_refresh_delegates(backend, inner):
    backend.refresh("job", "owner1", 30)
    inner.refresh.assert_called_once_with("job", "owner1", 30)


def test_inner_failure_still_counts(inner):
    inner.acquire.return_value = False
    limiter = RateLimiter(max_attempts=2, window=10)
    b = RateLimitedBackend(inner, limiter)
    assert b.acquire("job", "o", 10) is False
    assert b.acquire("job", "o", 10) is False
    assert b.acquire("job", "o", 10) is False  # rate limited now
    assert inner.acquire.call_count == 2
