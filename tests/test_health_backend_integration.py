"""Integration tests for HealthBackend over MemoryBackend."""
from __future__ import annotations

import time

import pytest

from schedlock.backends.health_backend import HealthBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    backend = HealthBackend(memory, failure_threshold=3, recovery_window=5.0)
    assert backend.acquire("job", "worker-1", 60) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "worker-1") is True
    assert backend.is_locked("job") is False


def test_second_worker_blocked_while_first_holds(memory):
    backend = HealthBackend(memory, failure_threshold=5, recovery_window=5.0)
    assert backend.acquire("job", "worker-1", 60) is True
    assert backend.acquire("job", "worker-2", 60) is False


def test_failure_threshold_marks_unhealthy(memory):
    backend = HealthBackend(memory, failure_threshold=2, recovery_window=60.0)
    # Lock held by another owner so acquires fail
    memory.acquire("job", "holder", 300)
    backend.acquire("job", "worker-1", 10)
    backend.acquire("job", "worker-1", 10)
    assert backend.is_healthy is False


def test_recovery_resets_health(memory):
    backend = HealthBackend(memory, failure_threshold=2, recovery_window=0.05)
    memory.acquire("job", "holder", 300)
    backend.acquire("job", "worker-1", 10)
    backend.acquire("job", "worker-1", 10)
    assert backend.is_healthy is False
    time.sleep(0.1)
    assert backend.is_healthy is True


def test_health_backend_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    backend = HealthBackend(ns, failure_threshold=3, recovery_window=5.0)
    assert backend.acquire("task", "w1", 60) is True
    assert backend.release("task", "w1") is True
