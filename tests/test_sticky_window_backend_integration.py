"""Integration tests for StickyWindowBackend over MemoryBackend."""

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.sticky_window_backend import StickyWindowBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_within_window(memory):
    b = StickyWindowBackend(memory, schedule_fn=lambda: True)
    assert b.acquire("job", "worker-1", 60) is True
    assert b.is_locked("job") is True
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False


def test_acquire_blocked_outside_window(memory):
    b = StickyWindowBackend(memory, schedule_fn=lambda: False)
    assert b.acquire("job", "worker-1", 60) is False
    assert b.is_locked("job") is False


def test_second_worker_blocked_while_first_holds(memory):
    b = StickyWindowBackend(memory, schedule_fn=lambda: True)
    assert b.acquire("job", "worker-1", 60) is True
    assert b.acquire("job", "worker-2", 60) is False


def test_sticky_owner_reacquires_after_window_closes(memory):
    open_flag = [True]
    b = StickyWindowBackend(memory, schedule_fn=lambda: open_flag[0])
    assert b.acquire("job", "worker-1", 60) is True
    open_flag[0] = False
    # Sticky owner can still acquire
    assert b.acquire("job", "worker-1", 60) is True


def test_new_owner_blocked_after_window_closes(memory):
    open_flag = [True]
    b = StickyWindowBackend(memory, schedule_fn=lambda: open_flag[0])
    b.acquire("job", "worker-1", 60)
    b.release("job", "worker-1")
    open_flag[0] = False
    # No sticky owner; window closed — blocked
    assert b.acquire("job", "worker-2", 60) is False


def test_reacquire_after_release_by_new_owner(memory):
    b = StickyWindowBackend(memory, schedule_fn=lambda: True)
    b.acquire("job", "worker-1", 60)
    b.release("job", "worker-1")
    assert b.acquire("job", "worker-2", 60) is True


def test_sticky_window_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    b = StickyWindowBackend(ns, schedule_fn=lambda: True)
    assert b.acquire("job", "worker-1", 60) is True
    assert b.is_locked("job") is True
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False
