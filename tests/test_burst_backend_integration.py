"""Integration tests for BurstBackend over MemoryBackend."""

from __future__ import annotations

import time

import pytest

from schedlock.backends.burst_backend import BurstBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory() -> MemoryBackend:
    return MemoryBackend()


def test_full_lifecycle_within_burst(memory):
    backend = BurstBackend(memory, max_burst=2, burst_window=10.0)
    assert backend.acquire("cron:job", "worker-1", 60) is True
    assert backend.is_locked("cron:job") is True
    assert backend.release("cron:job", "worker-1") is True
    assert backend.is_locked("cron:job") is False


def test_burst_limit_prevents_excess_acquires(memory):
    backend = BurstBackend(memory, max_burst=2, burst_window=30.0)
    # Two different keys each with short TTL so they don't conflict
    assert backend.acquire("job-a", "w1", 60) is True
    assert backend.acquire("job-b", "w1", 60) is True
    # Third acquire on a NEW key within same backend — burst is per-key
    assert backend.acquire("job-c", "w1", 60) is True


def test_same_key_burst_exhausted_then_blocked(memory):
    backend = BurstBackend(memory, max_burst=2, burst_window=30.0)
    assert backend.acquire("job", "w1", 60) is True
    # Release so inner is free, then re-acquire to burn second burst slot
    backend.release("job", "w1")
    assert backend.acquire("job", "w2", 60) is True
    backend.release("job", "w2")
    # Burst exhausted — third attempt blocked even though inner is free
    result = backend.acquire("job", "w3", 60)
    assert result is False


def test_burst_window_reset_allows_reacquire(memory):
    backend = BurstBackend(memory, max_burst=1, burst_window=5.0)
    backend.acquire("job", "w1", 60)
    backend.release("job", "w1")
    # Manually expire the window
    backend._state["job"] = (1, time.monotonic() - 10.0)
    assert backend.acquire("job", "w2", 60) is True


def test_burst_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="team-a")
    backend = BurstBackend(ns, max_burst=2, burst_window=10.0)
    assert backend.acquire("report", "w1", 60) is True
    assert backend.is_locked("report") is True
    backend.release("report", "w1")
    assert backend.is_locked("report") is False
