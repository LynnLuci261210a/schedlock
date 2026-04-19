import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.blacklist_backend import BlacklistBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_allowed_owner_full_lifecycle(memory):
    backend = BlacklistBackend(memory, {"blocked"})
    assert backend.acquire("job", "worker-1", 60) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "worker-1") is True
    assert backend.is_locked("job") is False


def test_blacklisted_owner_cannot_acquire(memory):
    backend = BlacklistBackend(memory, {"blocked"})
    assert backend.acquire("job", "blocked", 60) is False
    assert backend.is_locked("job") is False


def test_blacklisted_owner_cannot_release_held_lock(memory):
    backend = BlacklistBackend(memory, {"blocked"})
    # Acquire via inner directly
    memory.acquire("job", "blocked", 60)
    assert backend.release("job", "blocked") is False
    # Lock still held in inner
    assert memory.is_locked("job") is True


def test_blacklist_over_namespaced(memory):
    ns = NamespacedBackend(memory, "ns")
    backend = BlacklistBackend(ns, {"evil"})
    assert backend.acquire("job", "good", 60) is True
    assert backend.acquire("job", "evil", 60) is False
    assert backend.release("job", "good") is True
    assert backend.is_locked("job") is False


def test_multiple_owners_one_blacklisted(memory):
    backend = BlacklistBackend(memory, {"bad"})
    assert backend.acquire("job", "bad", 60) is False
    assert backend.acquire("job", "good", 60) is True
    assert backend.is_locked("job") is True
