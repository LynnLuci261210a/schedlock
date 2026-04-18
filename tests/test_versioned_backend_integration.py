import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.versioned_backend import VersionedBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_increments_version(memory):
    backend = VersionedBackend(memory)
    assert backend.acquire("job", "w1", 60)
    assert backend.version_of("job") == 1
    backend.release("job", "w1")
    assert backend.acquire("job", "w2", 60)
    assert backend.version_of("job") == 2


def test_blocked_acquire_does_not_bump_version(memory):
    backend = VersionedBackend(memory)
    backend.acquire("job", "w1", 60)
    result = backend.acquire("job", "w2", 60)
    assert result is False
    assert backend.version_of("job") == 1


def test_versioned_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    backend = VersionedBackend(ns)
    backend.acquire("report", "w", 30)
    assert backend.version_of("report") == 1
    assert backend.is_locked("report")


def test_multiple_keys_independent_versions(memory):
    backend = VersionedBackend(memory)
    for _ in range(3):
        backend.acquire("a", "w", 10)
        backend.release("a", "w")
    backend.acquire("b", "w", 10)
    assert backend.version_of("a") == 3
    assert backend.version_of("b") == 1
    assert backend.version_of("c") == 0
