"""Integration tests for SuspendableBackend over MemoryBackend."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.suspendable_backend import SuspendableBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_when_active(memory):
    backend = SuspendableBackend(memory)
    assert backend.acquire("cron.daily", "worker-1", 60) is True
    assert backend.is_locked("cron.daily") is True
    assert backend.release("cron.daily", "worker-1") is True
    assert backend.is_locked("cron.daily") is False


def test_acquire_blocked_while_suspended(memory):
    backend = SuspendableBackend(memory)
    backend.suspend()
    assert backend.acquire("cron.daily", "worker-1", 60) is False
    assert backend.is_locked("cron.daily") is False


def test_held_lock_releasable_while_suspended(memory):
    backend = SuspendableBackend(memory)
    assert backend.acquire("cron.daily", "worker-1", 60) is True
    backend.suspend()
    # lock is still held by inner backend
    assert backend.is_locked("cron.daily") is True
    # release should still work
    assert backend.release("cron.daily", "worker-1") is True
    assert backend.is_locked("cron.daily") is False


def test_resume_allows_acquire_again(memory):
    backend = SuspendableBackend(memory)
    backend.suspend()
    assert backend.acquire("cron.daily", "worker-1", 60) is False
    backend.resume()
    assert backend.acquire("cron.daily", "worker-1", 60) is True


def test_second_worker_blocked_by_inner_while_active(memory):
    backend = SuspendableBackend(memory)
    assert backend.acquire("cron.daily", "worker-1", 60) is True
    assert backend.acquire("cron.daily", "worker-2", 60) is False


def test_suspendable_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns")
    backend = SuspendableBackend(ns)
    assert backend.acquire("job", "w1", 30) is True
    assert backend.is_locked("job") is True
    backend.suspend()
    assert backend.acquire("job", "w2", 30) is False
    assert backend.release("job", "w1") is True
