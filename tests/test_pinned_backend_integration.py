"""Integration tests for PinnedBackend over MemoryBackend."""

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.pinned_backend import PinnedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_pinned_full_lifecycle(memory):
    backend = PinnedBackend(memory, allowed_owners=["svc-a"])
    assert backend.acquire("cron:daily", "svc-a", 30) is True
    assert backend.is_locked("cron:daily") is True
    assert backend.release("cron:daily", "svc-a") is True
    assert backend.is_locked("cron:daily") is False


def test_pinned_second_owner_blocked_while_first_holds(memory):
    backend = PinnedBackend(memory, allowed_owners=["svc-a", "svc-b"])
    backend.acquire("cron:daily", "svc-a", 30)
    # svc-b is allowed but lock is held by svc-a
    assert backend.acquire("cron:daily", "svc-b", 30) is False


def test_unknown_owner_never_acquires(memory):
    backend = PinnedBackend(memory, allowed_owners=["svc-a"])
    for _ in range(3):
        assert backend.acquire("cron:daily", "rogue", 30) is False
    assert memory.is_locked("cron:daily") is False


def test_pinned_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    backend = PinnedBackend(ns, allowed_owners=["worker-1"])
    assert backend.acquire("task", "worker-1", 60) is True
    # Underlying memory should hold namespaced key
    assert memory.is_locked("prod:task") is True
    assert backend.release("task", "worker-1") is True
    assert memory.is_locked("prod:task") is False
