"""Integration tests for ScheduledBackend over MemoryBackend."""

from __future__ import annotations

import datetime

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.scheduled_backend import ScheduledBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def _always_open(_dt: datetime.datetime) -> bool:
    return True


def _always_closed(_dt: datetime.datetime) -> bool:
    return False


def test_full_lifecycle_when_open(memory):
    b = ScheduledBackend(memory, schedule_fn=_always_open)
    assert b.acquire("job", "w1", 60) is True
    assert b.is_locked("job") is True
    assert b.release("job", "w1") is True
    assert b.is_locked("job") is False


def test_acquire_blocked_when_closed(memory):
    b = ScheduledBackend(memory, schedule_fn=_always_closed)
    assert b.acquire("job", "w1", 60) is False
    assert b.is_locked("job") is False


def test_release_succeeds_even_when_closed(memory):
    # Acquire directly via the inner backend so the lock is held.
    memory.acquire("job", "w1", 60)
    b = ScheduledBackend(memory, schedule_fn=_always_closed)
    # Release should succeed even though schedule is closed.
    assert b.release("job", "w1") is True
    assert b.is_locked("job") is False


def test_schedule_toggle_allows_then_blocks(memory):
    allowed = [True]
    b = ScheduledBackend(memory, schedule_fn=lambda dt: allowed[0])

    assert b.acquire("job", "w1", 60) is True
    b.release("job", "w1")

    allowed[0] = False
    assert b.acquire("job", "w2", 60) is False


def test_scheduled_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    b = ScheduledBackend(ns, schedule_fn=_always_open)
    assert b.acquire("report", "w1", 30) is True
    assert b.is_locked("report") is True
    assert b.release("report", "w1") is True
    assert b.is_locked("report") is False
