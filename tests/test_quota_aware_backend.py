"""Tests for QuotaAwareBackend."""

import time
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.quota_aware_backend import QuotaAwareBackend
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
    return QuotaAwareBackend(inner, max_per_owner=3, window=60.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        QuotaAwareBackend("not-a-backend")


def test_requires_positive_max_per_owner(inner):
    with pytest.raises(ValueError):
        QuotaAwareBackend(inner, max_per_owner=0)


def test_requires_positive_window(inner):
    with pytest.raises(ValueError):
        QuotaAwareBackend(inner, window=0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_per_owner_property(backend):
    assert backend.max_per_owner == 3


def test_window_property(backend):
    assert backend.window == 60.0


def test_acquire_within_quota(inner, backend):
    for _ in range(3):
        assert backend.acquire("job", "worker-1", 30) is True
    assert inner.acquire.call_count == 3


def test_acquire_blocked_after_quota_exceeded(inner, backend):
    for _ in range(3):
        backend.acquire("job", "worker-1", 30)
    result = backend.acquire("job", "worker-1", 30)
    assert result is False
    assert inner.acquire.call_count == 3  # 4th call never reached inner


def test_quota_used_tracks_successful_acquires(inner, backend):
    backend.acquire("job", "worker-1", 30)
    backend.acquire("job", "worker-1", 30)
    assert backend.quota_used("worker-1") == 2


def test_quota_remaining(inner, backend):
    backend.acquire("job", "worker-1", 30)
    assert backend.quota_remaining("worker-1") == 2


def test_failed_inner_acquire_does_not_increment_quota(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 30)
    assert backend.quota_used("worker-1") == 0


def test_different_owners_have_independent_quotas(inner, backend):
    for _ in range(3):
        backend.acquire("job", "worker-1", 30)
    # worker-2 should still be allowed
    assert backend.acquire("job", "worker-2", 30) is True


def test_release_delegates_to_inner(inner, backend):
    backend.release("job", "worker-1")
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates_to_inner(inner, backend):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates_to_inner(inner, backend):
    backend.refresh("job", "worker-1", 60)
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_quota_resets_after_window(inner):
    b = QuotaAwareBackend(inner, max_per_owner=2, window=0.05)
    b.acquire("job", "w", 30)
    b.acquire("job", "w", 30)
    assert b.quota_remaining("w") == 0
    time.sleep(0.1)
    assert b.quota_remaining("w") == 2
    assert b.acquire("job", "w", 30) is True
