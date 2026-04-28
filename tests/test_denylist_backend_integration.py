import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.denylist_backend import DenylistBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_allowed_owner(memory):
    backend = DenylistBackend(memory)
    assert backend.acquire("job", "worker-1", 60) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "worker-1") is True
    assert backend.is_locked("job") is False


def test_denied_owner_cannot_acquire(memory):
    backend = DenylistBackend(memory)
    backend.deny("bad-actor")
    assert backend.acquire("job", "bad-actor", 60) is False
    assert backend.is_locked("job") is False


def test_denied_owner_cannot_steal_held_lock(memory):
    backend = DenylistBackend(memory)
    assert backend.acquire("job", "worker-1", 60) is True
    backend.deny("worker-1")
    # worker-1 is now denied; it cannot release its own lock
    assert backend.release("job", "worker-1") is False
    # lock is still held by worker-1 at the inner level
    assert memory.is_locked("job") is True


def test_second_allowed_worker_blocked_while_first_holds(memory):
    backend = DenylistBackend(memory)
    assert backend.acquire("job", "worker-1", 60) is True
    # worker-2 is allowed but lock is taken
    assert backend.acquire("job", "worker-2", 60) is False


def test_per_key_deny_does_not_bleed_across_keys(memory):
    backend = DenylistBackend(memory)
    backend.deny("worker-x", key="job-a")
    assert backend.acquire("job-a", "worker-x", 60) is False
    assert backend.acquire("job-b", "worker-x", 60) is True


def test_denylist_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    backend = DenylistBackend(ns)
    backend.deny("blocked")
    assert backend.acquire("task", "blocked", 30) is False
    assert backend.acquire("task", "allowed", 30) is True
    assert backend.release("task", "allowed") is True
