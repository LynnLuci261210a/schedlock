"""Integration tests for CircuitRetryBackend over MemoryBackend."""
from __future__ import annotations

import pytest
from schedlock.backends.circuit_retry_backend import CircuitRetryBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    backend = CircuitRetryBackend(memory, retries=2, delay=0.0, failure_threshold=5, reset_timeout=30.0)
    assert backend.acquire("job", "worker-1", 60) is True
    assert backend.is_locked("job") is True
    assert backend.acquire("job", "worker-2", 60) is False
    assert backend.release("job", "worker-1") is True
    assert backend.is_locked("job") is False


def test_second_acquire_blocked(memory):
    backend = CircuitRetryBackend(memory, retries=1, delay=0.0, failure_threshold=10, reset_timeout=30.0)
    backend.acquire("job", "w1", 60)
    assert backend.acquire("job", "w2", 60) is False


def test_reacquire_after_release(memory):
    backend = CircuitRetryBackend(memory, retries=1, delay=0.0, failure_threshold=10, reset_timeout=30.0)
    backend.acquire("job", "w1", 60)
    backend.release("job", "w1")
    assert backend.acquire("job", "w2", 60) is True


def test_circuit_retry_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    backend = CircuitRetryBackend(ns, retries=2, delay=0.0, failure_threshold=5, reset_timeout=30.0)
    assert backend.acquire("nightly", "w1", 30) is True
    assert backend.is_locked("nightly") is True
    assert backend.release("nightly", "w1") is True
    assert backend.is_locked("nightly") is False


def test_circuit_stays_closed_on_clean_operations(memory):
    backend = CircuitRetryBackend(memory, retries=3, delay=0.0, failure_threshold=3, reset_timeout=30.0)
    for i in range(10):
        backend.acquire(f"job-{i}", "worker", 30)
        backend.release(f"job-{i}", "worker")
    assert backend.is_open is False
