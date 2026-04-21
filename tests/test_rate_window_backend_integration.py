"""Integration tests for RateWindowBackend over MemoryBackend."""

import time
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.rate_window_backend import RateWindowBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    b = RateWindowBackend(memory, max_acquires=3, window=60.0)
    assert b.acquire("job", "worker-1", 30) is True
    assert b.is_locked("job") is True
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False


def test_rate_limit_enforced_over_memory(memory):
    b = RateWindowBackend(memory, max_acquires=2, window=60.0)
    assert b.acquire("job", "worker-1", 30) is True
    b.release("job", "worker-1")
    assert b.acquire("job", "worker-2", 30) is True
    b.release("job", "worker-2")
    # Third attempt within window should be blocked
    assert b.acquire("job", "worker-3", 30) is False


def test_window_reset_allows_new_acquires(memory):
    b = RateWindowBackend(memory, max_acquires=1, window=0.1)
    assert b.acquire("job", "worker-1", 30) is True
    b.release("job", "worker-1")
    time.sleep(0.15)
    assert b.acquire("job", "worker-2", 30) is True


def test_independent_keys_not_cross_limited(memory):
    b = RateWindowBackend(memory, max_acquires=1, window=60.0)
    assert b.acquire("job-a", "worker", 30) is True
    assert b.acquire("job-b", "worker", 30) is True


def test_rate_window_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns")
    b = RateWindowBackend(ns, max_acquires=2, window=60.0)
    assert b.acquire("job", "worker-1", 30) is True
    b.release("job", "worker-1")
    assert b.acquire("job", "worker-2", 30) is True
    b.release("job", "worker-2")
    assert b.acquire("job", "worker-3", 30) is False
