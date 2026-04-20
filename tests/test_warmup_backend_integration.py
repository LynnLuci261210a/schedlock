"""Integration tests for WarmupBackend over MemoryBackend."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.warmup_backend import WarmupBackend


@pytest.fixture()
def memory() -> MemoryBackend:
    return MemoryBackend()


def test_acquire_blocked_before_warmup_elapses(memory: MemoryBackend) -> None:
    backend = WarmupBackend(memory, warmup_seconds=60.0)
    assert backend.acquire("cron:job", "worker-1", 30) is False
    assert memory.is_locked("cron:job") is False


def test_acquire_succeeds_after_warmup(memory: MemoryBackend) -> None:
    backend = WarmupBackend(memory, warmup_seconds=1.0)
    with patch(
        "schedlock.backends.warmup_backend.time.monotonic",
        return_value=backend._started_at + 2.0,
    ):
        acquired = backend.acquire("cron:job", "worker-1", 30)
    assert acquired is True
    assert memory.is_locked("cron:job") is True


def test_release_after_acquire(memory: MemoryBackend) -> None:
    backend = WarmupBackend(memory, warmup_seconds=1.0)
    with patch(
        "schedlock.backends.warmup_backend.time.monotonic",
        return_value=backend._started_at + 5.0,
    ):
        backend.acquire("cron:job", "worker-1", 30)

    released = backend.release("cron:job", "worker-1")
    assert released is True
    assert memory.is_locked("cron:job") is False


def test_warmup_over_namespaced(memory: MemoryBackend) -> None:
    ns = NamespacedBackend(memory, namespace="svc")
    backend = WarmupBackend(ns, warmup_seconds=1.0)
    with patch(
        "schedlock.backends.warmup_backend.time.monotonic",
        return_value=backend._started_at + 2.0,
    ):
        assert backend.acquire("job", "worker-1", 30) is True
    assert ns.is_locked("job") is True


def test_two_workers_only_one_warmed_up(memory: MemoryBackend) -> None:
    """Worker-1 is warmed up; worker-2 is still in warmup — only worker-1 acquires."""
    b1 = WarmupBackend(memory, warmup_seconds=1.0)
    b2 = WarmupBackend(memory, warmup_seconds=60.0)

    with patch(
        "schedlock.backends.warmup_backend.time.monotonic",
        return_value=b1._started_at + 2.0,
    ):
        assert b1.acquire("cron:job", "worker-1", 30) is True

    # b2 still in warmup — should not acquire
    assert b2.acquire("cron:job", "worker-2", 30) is False
