"""Integration tests for MaintenanceBackend over MemoryBackend."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.maintenance_backend import MaintenanceBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle_when_not_in_maintenance(memory):
    backend = MaintenanceBackend(memory)
    assert backend.acquire("task", "w1", 60) is True
    assert backend.is_locked("task") is True
    assert backend.release("task", "w1") is True
    assert backend.is_locked("task") is False


def test_acquire_blocked_while_in_maintenance(memory):
    backend = MaintenanceBackend(memory)
    backend.enter_maintenance()
    assert backend.acquire("task", "w1", 60) is False
    assert backend.is_locked("task") is False


def test_held_lock_releasable_during_maintenance(memory):
    """A lock acquired before maintenance began can still be released."""
    backend = MaintenanceBackend(memory)
    assert backend.acquire("task", "w1", 60) is True
    backend.enter_maintenance()
    # release must still work
    assert backend.release("task", "w1") is True
    assert backend.is_locked("task") is False


def test_second_worker_cannot_acquire_during_maintenance(memory):
    backend = MaintenanceBackend(memory)
    assert backend.acquire("task", "w1", 60) is True
    backend.enter_maintenance()
    assert backend.acquire("task", "w2", 60) is False


def test_resume_after_maintenance_allows_acquire(memory):
    backend = MaintenanceBackend(memory)
    backend.enter_maintenance()
    assert backend.acquire("task", "w1", 60) is False
    backend.exit_maintenance()
    assert backend.acquire("task", "w1", 60) is True


def test_maintenance_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="svc")
    backend = MaintenanceBackend(ns, reason="rolling-deploy")
    assert backend.acquire("job", "w1", 30) is True
    backend.enter_maintenance()
    assert backend.acquire("job", "w2", 30) is False
    backend.exit_maintenance()
    assert backend.release("job", "w1") is True
    assert backend.acquire("job", "w2", 30) is True
