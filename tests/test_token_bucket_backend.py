"""Unit tests for TokenBucketBackend."""
import time
from unittest.mock import MagicMock, patch

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.token_bucket_backend import TokenBucketBackend


@pytest.fixture()
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture()
def backend(inner):
    return TokenBucketBackend(inner, rate=5.0, window=1.0, burst=5)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        TokenBucketBackend(object(), rate=1.0)  # type: ignore[arg-type]


def test_requires_positive_rate(inner):
    with pytest.raises(ValueError):
        TokenBucketBackend(inner, rate=0)


def test_requires_positive_window(inner):
    with pytest.raises(ValueError):
        TokenBucketBackend(inner, rate=1.0, window=0)


def test_requires_burst_at_least_one(inner):
    with pytest.raises(ValueError):
        TokenBucketBackend(inner, rate=1.0, burst=0.5)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_rate_property(inner, backend):
    assert backend.rate == 5.0


def test_burst_defaults_to_rate(inner):
    b = TokenBucketBackend(inner, rate=3.0)
    assert b.burst == 3.0


def test_acquire_success_within_burst(inner, backend):
    assert backend.acquire("job", "owner-1", 60) is True
    inner.acquire.assert_called_once_with("job", "owner-1", 60)


def test_acquire_denied_when_no_tokens(inner):
    b = TokenBucketBackend(inner, rate=1.0, burst=2)
    # exhaust tokens
    b.acquire("job", "owner-1", 60)
    b.acquire("job", "owner-1", 60)
    result = b.acquire("job", "owner-1", 60)
    assert result is False
    assert inner.acquire.call_count == 2


def test_tokens_refill_over_time(inner):
    b = TokenBucketBackend(inner, rate=10.0, window=1.0, burst=1)
    b.acquire("job", "o", 30)  # consume the single token
    assert b.acquire("job", "o", 30) is False  # no token yet
    # Simulate time passing: 0.2 s -> 10 tokens/s * 0.2 s = 2 new tokens (capped at 1)
    with patch("schedlock.backends.token_bucket_backend.time.monotonic", return_value=b._last_refill + 0.2):
        result = b.acquire("job", "o", 30)
    assert result is True


def test_release_delegates(inner, backend):
    backend.release("job", "owner-1")
    inner.release.assert_called_once_with("job", "owner-1")


def test_is_locked_delegates(inner, backend):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    backend.refresh("job", "owner-1", 60)
    inner.refresh.assert_called_once_with("job", "owner-1", 60)
