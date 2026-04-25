"""Integration tests for ExpiryJitterBackend over MemoryBackend."""

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.expiry_jitter_backend import ExpiryJitterBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    backend = ExpiryJitterBackend(memory, max_jitter=2.0, seed=1)
    assert backend.acquire("job", "worker-1", ttl=60) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "worker-1") is True
    assert backend.is_locked("job") is False


def test_second_worker_blocked_while_first_holds(memory):
    backend = ExpiryJitterBackend(memory, max_jitter=2.0, seed=2)
    assert backend.acquire("job", "worker-1", ttl=60) is True
    assert backend.acquire("job", "worker-2", ttl=60) is False


def test_reacquire_after_release(memory):
    backend = ExpiryJitterBackend(memory, max_jitter=2.0, seed=3)
    backend.acquire("job", "worker-1", ttl=60)
    backend.release("job", "worker-1")
    assert backend.acquire("job", "worker-2", ttl=60) is True


def test_jitter_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    backend = ExpiryJitterBackend(ns, max_jitter=3.0, seed=7)
    assert backend.acquire("job", "worker-1", ttl=30) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "worker-1") is True
    assert backend.is_locked("job") is False


def test_independent_keys_not_cross_affected(memory):
    backend = ExpiryJitterBackend(memory, max_jitter=1.0, seed=5)
    assert backend.acquire("job-a", "worker-1", ttl=60) is True
    assert backend.acquire("job-b", "worker-2", ttl=60) is True
    assert backend.is_locked("job-a") is True
    assert backend.is_locked("job-b") is True
