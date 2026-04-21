"""Integration tests for GraceBackend over MemoryBackend."""

from __future__ import annotations

import time
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.grace_backend import GraceBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_with_grace(memory):
    b = GraceBackend(memory, grace_seconds=0.05)
    assert b.acquire("task", "w1") is True
    assert b.is_locked("task") is True
    assert b.release("task", "w1") is True
    # Grace period active — second worker blocked
    assert b.acquire("task", "w2") is False
    assert b.is_locked("task") is True
    # After grace expires, second worker can acquire
    time.sleep(0.1)
    assert b.acquire("task", "w2") is True
    assert b.release("task", "w2") is True


def test_second_worker_blocked_while_first_holds(memory):
    b = GraceBackend(memory, grace_seconds=0.05)
    assert b.acquire("task", "w1") is True
    assert b.acquire("task", "w2") is False


def test_grace_does_not_leak_across_keys(memory):
    b = GraceBackend(memory, grace_seconds=1.0)
    b.acquire("key-x", "w1")
    b.release("key-x", "w1")
    # key-y completely independent
    assert b.acquire("key-y", "w1") is True
    assert b.release("key-y", "w1") is True


def test_grace_backend_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="svc")
    b = GraceBackend(ns, grace_seconds=0.05)
    assert b.acquire("job", "w1") is True
    assert b.release("job", "w1") is True
    assert b.acquire("job", "w2") is False
    time.sleep(0.1)
    assert b.acquire("job", "w2") is True


def test_reacquire_by_same_owner_after_grace(memory):
    b = GraceBackend(memory, grace_seconds=0.05)
    b.acquire("job", "w1")
    b.release("job", "w1")
    time.sleep(0.1)
    assert b.acquire("job", "w1") is True
    assert b.is_locked("job") is True
