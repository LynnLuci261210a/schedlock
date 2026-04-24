"""Integration tests for DampeningBackend over MemoryBackend."""
from __future__ import annotations

import time

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.dampening_backend import DampeningBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    backend = DampeningBackend(memory, dampening_seconds=1.0)
    assert backend.acquire("cron:cleanup", "worker-1", 30) is True
    assert backend.is_locked("cron:cleanup") is True
    assert backend.release("cron:cleanup", "worker-1") is True
    assert backend.is_locked("cron:cleanup") is False


def test_failed_acquire_suppresses_retries(memory):
    backend = DampeningBackend(memory, dampening_seconds=1.0)
    # worker-1 holds the lock
    memory.acquire("cron:cleanup", "worker-1", 30)
    # worker-2 fails and gets dampened
    assert backend.acquire("cron:cleanup", "worker-2", 30) is False
    # Even after worker-1 releases, worker-2 is still dampened
    memory.release("cron:cleanup", "worker-1")
    assert backend.acquire("cron:cleanup", "worker-2", 30) is False


def test_dampening_lifts_and_allows_reacquire(memory):
    backend = DampeningBackend(memory, dampening_seconds=0.05)
    memory.acquire("cron:cleanup", "worker-1", 30)
    backend.acquire("cron:cleanup", "worker-2", 30)  # fails → dampened
    memory.release("cron:cleanup", "worker-1")
    time.sleep(0.1)
    # Dampening expired; worker-2 should now succeed
    assert backend.acquire("cron:cleanup", "worker-2", 30) is True


def test_dampening_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    backend = DampeningBackend(ns, dampening_seconds=1.0)
    assert backend.acquire("job", "worker-1", 30) is True
    assert backend.release("job", "worker-1") is True
    assert backend.is_locked("job") is False
