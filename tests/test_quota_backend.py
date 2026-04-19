import pytest
from unittest.mock import MagicMock
from schedlock.backends.quota_backend import QuotaBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    return MemoryBackend()


@pytest.fixture
def backend(inner):
    return QuotaBackend(inner, max_acquires=2, window=60.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        QuotaBackend("not-a-backend", max_acquires=2, window=60.0)


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_succeeds_within_quota(backend):
    assert backend.acquire("job", "worker-1", 30) is True


def test_acquire_blocked_after_quota_exceeded(backend):
    backend.acquire("job", "worker-1", 1)
    backend.release("job", "worker-1")
    backend.acquire("job", "worker-1", 1)
    backend.release("job", "worker-1")
    assert backend.acquire("job", "worker-1", 1) is False


def test_release_delegates(backend, inner):
    backend.acquire("job", "w1", 30)
    assert backend.release("job", "w1") is True
    assert inner.is_locked("job") is False


def test_is_locked_delegates(backend):
    assert backend.is_locked("job") is False
    backend.acquire("job", "w1", 30)
    assert backend.is_locked("job") is True


def test_quota_not_consumed_on_failed_acquire():
    inner = MemoryBackend()
    inner.acquire("job", "holder", 60)
    b = QuotaBackend(inner, max_acquires=1, window=60.0)
    # inner is locked so acquire fails; quota should not be consumed
    assert b.acquire("job", "other", 30) is False
    inner.release("job", "holder")
    assert b.acquire("job", "other", 30) is True


def test_independent_keys_have_independent_quotas(backend):
    backend.acquire("job-a", "w1", 1)
    backend.release("job-a", "w1")
    backend.acquire("job-a", "w1", 1)
    backend.release("job-a", "w1")
    assert backend.acquire("job-a", "w1", 1) is False
    assert backend.acquire("job-b", "w1", 1) is True
