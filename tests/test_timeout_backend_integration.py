"""Integration tests for TimeoutBackend over MemoryBackend."""
from __future__ import annotations

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.timeout_backend import TimeoutBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    tb = TimeoutBackend(memory, timeout_seconds=1.0)
    assert tb.acquire("cron:job", "worker-1", 30) is True
    assert tb.is_locked("cron:job") is True
    assert tb.release("cron:job", "worker-1") is True
    assert tb.is_locked("cron:job") is False


def test_second_worker_blocked_while_first_holds(memory):
    tb = TimeoutBackend(memory, timeout_seconds=1.0)
    assert tb.acquire("cron:job", "worker-1", 30) is True
    assert tb.acquire("cron:job", "worker-2", 30) is False


def test_reacquire_after_release(memory):
    tb = TimeoutBackend(memory, timeout_seconds=1.0)
    tb.acquire("cron:job", "worker-1", 30)
    tb.release("cron:job", "worker-1")
    assert tb.acquire("cron:job", "worker-2", 30) is True


def test_timeout_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    tb = TimeoutBackend(ns, timeout_seconds=1.0)
    assert tb.acquire("cron:job", "worker-1", 30) is True
    # Raw memory key should carry the namespace prefix.
    assert memory.is_locked("prod:cron:job") is True
    tb.release("cron:job", "worker-1")
    assert memory.is_locked("prod:cron:job") is False


def test_independent_keys_not_cross_blocked(memory):
    tb = TimeoutBackend(memory, timeout_seconds=1.0)
    assert tb.acquire("job:a", "worker-1", 30) is True
    assert tb.acquire("job:b", "worker-2", 30) is True
