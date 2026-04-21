"""Integration tests for TimeBoundedBackend over MemoryBackend."""

from __future__ import annotations

import time

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.time_bounded_backend import TimeBoundedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_basic_acquire_and_release(memory):
    b = TimeBoundedBackend(memory, max_hold_seconds=10)
    assert b.acquire("job", owner="w1", ttl=60) is True
    locked, owner = b.is_locked("job")
    assert locked is True
    assert owner == "w1"
    assert b.release("job", owner="w1") is True
    locked, _ = b.is_locked("job")
    assert locked is False


def test_second_worker_blocked_while_first_holds(memory):
    b = TimeBoundedBackend(memory, max_hold_seconds=10)
    assert b.acquire("job", owner="w1", ttl=60) is True
    assert b.acquire("job", owner="w2", ttl=60) is False


def test_overdue_lock_evicted_allows_second_worker(memory):
    b = TimeBoundedBackend(memory, max_hold_seconds=0.05)
    assert b.acquire("job", owner="w1", ttl=60) is True
    # Force the timestamp to appear old
    b._acquired_at["job"] = time.monotonic() - 1.0
    # w2 triggers eviction and then acquires
    assert b.acquire("job", owner="w2", ttl=60) is True
    locked, owner = b.is_locked("job")
    assert locked is True
    assert owner == "w2"


def test_is_locked_evicts_overdue(memory):
    b = TimeBoundedBackend(memory, max_hold_seconds=0.05)
    assert b.acquire("job", owner="w1", ttl=60) is True
    b._acquired_at["job"] = time.monotonic() - 1.0
    locked, _ = b.is_locked("job")
    # After eviction the memory backend should report unlocked
    assert locked is False


def test_release_clears_acquired_at(memory):
    b = TimeBoundedBackend(memory, max_hold_seconds=10)
    b.acquire("job", owner="w1", ttl=60)
    b.release("job", owner="w1")
    assert "job" not in b._acquired_at


def test_time_bounded_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="sched")
    b = TimeBoundedBackend(ns, max_hold_seconds=10)
    assert b.acquire("job", owner="w1", ttl=60) is True
    assert b.acquire("job", owner="w2", ttl=60) is False
    assert b.release("job", owner="w1") is True
    assert b.acquire("job", owner="w2", ttl=60) is True
