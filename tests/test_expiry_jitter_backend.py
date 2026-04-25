"""Tests for ExpiryJitterBackend."""

from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.expiry_jitter_backend import ExpiryJitterBackend


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
    return ExpiryJitterBackend(inner, max_jitter=10.0, seed=42)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        ExpiryJitterBackend(inner="not-a-backend")


def test_requires_positive_max_jitter(inner):
    with pytest.raises(ValueError, match="max_jitter must be a positive number"):
        ExpiryJitterBackend(inner, max_jitter=0)


def test_requires_numeric_max_jitter(inner):
    with pytest.raises(ValueError, match="max_jitter must be a positive number"):
        ExpiryJitterBackend(inner, max_jitter="five")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_jitter_property(inner):
    b = ExpiryJitterBackend(inner, max_jitter=3.5)
    assert b.max_jitter == 3.5


def test_acquire_delegates_with_adjusted_ttl(inner, backend):
    result = backend.acquire("mykey", "worker-1", ttl=60)
    assert result is True
    inner.acquire.assert_called_once()
    call_key, call_owner, call_ttl = inner.acquire.call_args[0]
    assert call_key == "mykey"
    assert call_owner == "worker-1"
    assert call_ttl >= 60
    assert call_ttl <= 70  # 60 + max_jitter(10)


def test_acquire_ttl_never_less_than_one(inner):
    b = ExpiryJitterBackend(inner, max_jitter=1.0, seed=0)
    b.acquire("k", "o", ttl=1)
    _, _, call_ttl = inner.acquire.call_args[0]
    assert call_ttl >= 1


def test_release_delegates(inner, backend):
    backend.release("mykey", "worker-1")
    inner.release.assert_called_once_with("mykey", "worker-1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("mykey") is True
    inner.is_locked.assert_called_once_with("mykey")


def test_refresh_delegates_with_adjusted_ttl(inner, backend):
    result = backend.refresh("mykey", "worker-1", ttl=30)
    assert result is True
    inner.refresh.assert_called_once()
    _, _, call_ttl = inner.refresh.call_args[0]
    assert call_ttl >= 30
    assert call_ttl <= 40


def test_seed_produces_deterministic_jitter(inner):
    b1 = ExpiryJitterBackend(inner, max_jitter=5.0, seed=99)
    b2 = ExpiryJitterBackend(inner, max_jitter=5.0, seed=99)
    b1.acquire("k", "o", ttl=10)
    b2.acquire("k", "o", ttl=10)
    ttl1 = inner.acquire.call_args_list[-2][0][2]
    ttl2 = inner.acquire.call_args_list[-1][0][2]
    assert ttl1 == ttl2
