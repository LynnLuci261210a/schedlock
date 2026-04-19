"""Integration tests for LeakyBucketBackend over MemoryBackend."""

import time
import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.leaky_bucket_backend import LeakyBucketBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_basic_acquire_and_release(memory):
    backend = LeakyBucketBackend(memory, rate=5, window=60.0)
    assert backend.acquire("job", "worker-1", 30) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "worker-1") is True
    assert backend.is_locked("job") is False


def test_rate_limit_enforced_over_memory(memory):
    backend = LeakyBucketBackend(memory, rate=2, window=60.0)
    assert backend.acquire("a", "w1", 30) is True
    backend.release("a", "w1")
    assert backend.acquire("b", "w1", 30) is True
    backend.release("b", "w1")
    # third acquire exceeds rate
    assert backend.acquire("c", "w1", 30) is False


def test_window_reset_allows_new_acquires(memory):
    backend = LeakyBucketBackend(memory, rate=1, window=0.1)
    assert backend.acquire("job", "w1", 30) is True
    backend.release("job", "w1")
    assert backend.acquire("job", "w1", 30) is False
    time.sleep(0.15)
    assert backend.acquire("job", "w1", 30) is True


def test_leaky_bucket_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="svc")
    backend = LeakyBucketBackend(ns, rate=2, window=60.0)
    assert backend.acquire("task", "w1", 30) is True
    assert backend.acquire("task2", "w1", 30) is True
    assert backend.acquire("task3", "w1", 30) is False
