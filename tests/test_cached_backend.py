"""Tests for CachedBackend."""

import time
import pytest
from unittest.mock import MagicMock
from schedlock.backends.cached_backend import CachedBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    return m


@pytest.fixture
def backend(inner):
    return CachedBackend(inner, cache_ttl=1.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        CachedBackend("not-a-backend")


def test_requires_positive_cache_ttl(inner):
    with pytest.raises(ValueError):
        CachedBackend(inner, cache_ttl=0)
    with pytest.raises(ValueError):
        CachedBackend(inner, cache_ttl=-1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_delegates_and_caches(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner1", 60)


def test_acquire_failure_does_not_cache(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "owner1", 60)
    inner.is_locked.return_value = False
    backend.is_locked("job")
    inner.is_locked.assert_called_once()


def test_is_locked_cached_after_acquire(inner, backend):
    inner.acquire.return_value = True
    backend.acquire("job", "owner1", 60)
    result = backend.is_locked("job")
    assert result is True
    inner.is_locked.assert_not_called()


def test_is_locked_cache_expires(inner):
    backend = CachedBackend(inner, cache_ttl=0.05)
    inner.acquire.return_value = True
    backend.acquire("job", "owner1", 60)
    time.sleep(0.1)
    inner.is_locked.return_value = True
    backend.is_locked("job")
    inner.is_locked.assert_called_once()


def test_release_invalidates_cache(inner, backend):
    inner.acquire.return_value = True
    backend.acquire("job", "owner1", 60)
    inner.release.return_value = True
    backend.release("job", "owner1")
    result = backend.is_locked("job")
    assert result is False
    inner.is_locked.assert_not_called()


def test_manual_invalidate_clears_cache(inner, backend):
    inner.acquire.return_value = True
    backend.acquire("job", "owner1", 60)
    backend.invalidate("job")
    inner.is_locked.return_value = False
    backend.is_locked("job")
    inner.is_locked.assert_called_once()


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    assert backend.refresh("job", "owner1", 60) is True
    inner.refresh.assert_called_once_with("job", "owner1", 60)
