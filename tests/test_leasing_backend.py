import time
import pytest
from unittest.mock import MagicMock
from schedlock.backends.leasing_backend import LeasingBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    inner.acquire.return_value = True
    inner.release.return_value = True
    inner.is_locked.return_value = True
    return LeasingBackend(inner, lease_seconds=10.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        LeasingBackend(object(), lease_seconds=5)


def test_requires_positive_lease_seconds(inner):
    with pytest.raises(ValueError):
        LeasingBackend(inner, lease_seconds=0)
    with pytest.raises(ValueError):
        LeasingBackend(inner, lease_seconds=-1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_lease_seconds_property(inner):
    b = LeasingBackend(inner, lease_seconds=42.0)
    assert b.lease_seconds == 42.0


def test_acquire_success_registers_lease(backend, inner):
    result = backend.acquire("job", "worker-1", ttl=30)
    assert result is True
    assert backend.lease_expires_at("job") is not None


def test_acquire_blocked_by_different_owner(backend, inner):
    backend.acquire("job", "worker-1", ttl=30)
    inner.acquire.return_value = True
    result = backend.acquire("job", "worker-2", ttl=30)
    assert result is False
    # inner should not have been called for the second acquire
    assert inner.acquire.call_count == 1


def test_same_owner_can_reacquire(backend, inner):
    backend.acquire("job", "worker-1", ttl=30)
    inner.acquire.return_value = True
    result = backend.acquire("job", "worker-1", ttl=30)
    assert result is True


def test_release_clears_lease(backend, inner):
    backend.acquire("job", "worker-1", ttl=30)
    backend.release("job", "worker-1")
    assert backend.lease_expires_at("job") is None


def test_release_by_wrong_owner_fails(backend, inner):
    backend.acquire("job", "worker-1", ttl=30)
    result = backend.release("job", "worker-2")
    assert result is False
    inner.release.assert_not_called()


def test_renew_updates_expiry(backend):
    backend.acquire("job", "worker-1", ttl=30)
    first_expiry = backend.lease_expires_at("job")
    time.sleep(0.05)
    renewed = backend.renew("job", "worker-1")
    assert renewed is True
    assert backend.lease_expires_at("job") > first_expiry


def test_renew_wrong_owner_returns_false(backend):
    backend.acquire("job", "worker-1", ttl=30)
    assert backend.renew("job", "worker-2") is False


def test_renew_missing_key_returns_false(backend):
    assert backend.renew("nonexistent", "worker-1") is False


def test_is_locked_false_after_lease_expiry(inner):
    b = LeasingBackend(inner, lease_seconds=0.05)
    inner.acquire.return_value = True
    b.acquire("job", "worker-1", ttl=1)
    time.sleep(0.1)
    inner.is_locked.return_value = True
    assert b.is_locked("job") is False


def test_acquire_allowed_after_lease_expiry(inner):
    b = LeasingBackend(inner, lease_seconds=0.05)
    inner.acquire.return_value = True
    b.acquire("job", "worker-1", ttl=1)
    time.sleep(0.1)
    result = b.acquire("job", "worker-2", ttl=1)
    assert result is True
