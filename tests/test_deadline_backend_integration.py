"""Integration tests for DeadlineBackend over MemoryBackend."""
from __future__ import annotations

import time

import pytest

from schedlock.backends.deadline_backend import DeadlineBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_acquire_before_deadline(memory):
    b = DeadlineBackend(memory, deadline=time.time() + 60)
    assert b.acquire("cron:job", "worker-1", 30) is True
    assert b.is_locked("cron:job") is True


def test_acquire_after_deadline(memory):
    b = DeadlineBackend(memory, deadline=time.time() - 1)
    assert b.acquire("cron:job", "worker-1", 30) is False
    assert b.is_locked("cron:job") is False


def test_release_after_deadline_still_works(memory):
    # Acquire directly on memory, then wrap with expired deadline
    memory.acquire("cron:job", "worker-1", 30)
    b = DeadlineBackend(memory, deadline=time.time() - 1)
    # release should still delegate
    assert b.release("cron:job", "worker-1") is True
    assert memory.is_locked("cron:job") is False


def test_deadline_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    b = DeadlineBackend(ns, deadline=time.time() + 60)
    assert b.acquire("job", "w1", 10) is True
    assert ns.is_locked("job") is True
    assert b.release("job", "w1") is True
    assert ns.is_locked("job") is False
