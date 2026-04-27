import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.allowlist_backend import AllowlistBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_allowed_owner_full_lifecycle(memory):
    backend = AllowlistBackend(memory, allowed={"worker-1"})
    assert backend.acquire("report", "worker-1", 60) is True
    assert backend.is_locked("report") is True
    assert backend.release("report", "worker-1") is True
    assert backend.is_locked("report") is False


def test_denied_owner_cannot_acquire(memory):
    backend = AllowlistBackend(memory, allowed={"worker-1"})
    with pytest.raises(PermissionError):
        backend.acquire("report", "intruder", 60)
    assert backend.is_locked("report") is False


def test_denied_owner_cannot_release_held_lock(memory):
    backend = AllowlistBackend(memory, allowed={"worker-1", "worker-2"})
    assert backend.acquire("report", "worker-1", 60) is True
    with pytest.raises(PermissionError):
        backend.release("report", "outsider")
    # lock still held
    assert backend.is_locked("report") is True


def test_second_allowed_worker_blocked_while_first_holds(memory):
    backend = AllowlistBackend(memory, allowed={"worker-1", "worker-2"})
    assert backend.acquire("report", "worker-1", 60) is True
    assert backend.acquire("report", "worker-2", 60) is False


def test_allowlist_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    backend = AllowlistBackend(ns, allowed={"alice"})
    assert backend.acquire("job", "alice", 30) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "alice") is True
