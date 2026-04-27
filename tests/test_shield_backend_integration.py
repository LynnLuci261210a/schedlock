"""Integration tests for ShieldBackend over MemoryBackend."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.shield_backend import ShieldBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle_when_not_shielded(memory):
    backend = ShieldBackend(memory)
    assert backend.acquire("cron:cleanup", "worker-1", 60) is True
    assert backend.is_locked("cron:cleanup") is True
    assert backend.release("cron:cleanup", "worker-1") is True
    assert backend.is_locked("cron:cleanup") is False


def test_acquire_blocked_while_shielded(memory):
    backend = ShieldBackend(memory)
    backend.raise_shield()
    assert backend.acquire("cron:cleanup", "worker-1", 60) is False
    assert backend.is_locked("cron:cleanup") is False


def test_lower_shield_allows_acquire(memory):
    backend = ShieldBackend(memory)
    backend.raise_shield()
    assert backend.acquire("cron:cleanup", "worker-1", 60) is False
    backend.lower_shield()
    assert backend.acquire("cron:cleanup", "worker-1", 60) is True
    assert backend.is_locked("cron:cleanup") is True


def test_held_lock_releasable_while_shielded(memory):
    backend = ShieldBackend(memory)
    assert backend.acquire("cron:cleanup", "worker-1", 60) is True
    backend.raise_shield()
    # release should still work even with shield raised
    assert backend.release("cron:cleanup", "worker-1") is True
    assert backend.is_locked("cron:cleanup") is False


def test_second_worker_blocked_while_first_holds(memory):
    backend = ShieldBackend(memory)
    assert backend.acquire("cron:report", "worker-1", 60) is True
    assert backend.acquire("cron:report", "worker-2", 60) is False


def test_shield_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    backend = ShieldBackend(ns)
    backend.raise_shield()
    assert backend.acquire("job", "w", 30) is False
    backend.lower_shield()
    assert backend.acquire("job", "w", 30) is True
    assert backend.release("job", "w") is True
