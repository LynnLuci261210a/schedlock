"""Integration tests for PausedBackend over a real MemoryBackend."""

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.paused_backend import PausedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle_when_not_paused(memory):
    b = PausedBackend(memory)
    assert b.acquire("job", "worker-1", 30) is True
    assert b.is_locked("job") is True
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False


def test_acquire_blocked_while_paused(memory):
    b = PausedBackend(memory)
    b.pause()
    assert b.acquire("job", "worker-1", 30) is False
    assert b.is_locked("job") is False


def test_resume_allows_acquire(memory):
    b = PausedBackend(memory)
    b.pause()
    b.resume()
    assert b.acquire("job", "worker-1", 30) is True
    assert b.is_locked("job") is True


def test_held_lock_releasable_while_paused(memory):
    b = PausedBackend(memory)
    # Acquire while unpaused, then pause, then release.
    assert b.acquire("job", "worker-1", 30) is True
    b.pause()
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False


def test_paused_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns")
    b = PausedBackend(ns)
    assert b.acquire("job", "worker-1", 30) is True
    b.pause()
    assert b.acquire("job", "worker-2", 30) is False
    b.resume()
    # Still locked by worker-1.
    assert b.acquire("job", "worker-2", 30) is False
    assert b.release("job", "worker-1") is True
    assert b.acquire("job", "worker-2", 30) is True
