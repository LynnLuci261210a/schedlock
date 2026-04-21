"""Integration tests for TokenBucketBackend over MemoryBackend."""
import time
from unittest.mock import patch

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.token_bucket_backend import TokenBucketBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    b = TokenBucketBackend(memory, rate=10.0, burst=10)
    assert b.acquire("job", "worker-1", 60) is True
    assert b.is_locked("job") is True
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False


def test_burst_exhausted_blocks_further_acquires(memory):
    b = TokenBucketBackend(memory, rate=2.0, window=1.0, burst=2)
    # Two acquires succeed (different keys to avoid memory-level blocking)
    assert b.acquire("job-a", "w1", 60) is True
    assert b.acquire("job-b", "w2", 60) is True
    # Third is denied by token bucket before hitting memory
    assert b.acquire("job-c", "w3", 60) is False


def test_refill_allows_new_acquire_after_window(memory):
    b = TokenBucketBackend(memory, rate=1.0, window=1.0, burst=1)
    assert b.acquire("job", "worker-1", 60) is True
    assert b.release("job", "worker-1") is True
    # No tokens left yet for a second acquire immediately
    assert b.acquire("job", "worker-2", 60) is False
    # Advance time by 1 second to refill one token
    with patch(
        "schedlock.backends.token_bucket_backend.time.monotonic",
        return_value=b._last_refill + 1.1,
    ):
        result = b.acquire("job", "worker-2", 60)
    assert result is True


def test_token_bucket_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="svc")
    b = TokenBucketBackend(ns, rate=5.0, burst=5)
    assert b.acquire("task", "w1", 30) is True
    assert b.is_locked("task") is True
    assert b.release("task", "w1") is True
    assert b.is_locked("task") is False
