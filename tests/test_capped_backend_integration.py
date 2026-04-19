"""Integration tests for CappedBackend over MemoryBackend."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.capped_backend import CappedBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_single_holder(memory):
    b = CappedBackend(memory, max_holders=1)
    assert b.acquire("task", "w1", 60) is True
    assert b.is_locked("task") is True
    assert b.release("task", "w1") is True
    assert b.is_locked("task") is False


def test_second_worker_unblocked_after_release(memory):
    b = CappedBackend(memory, max_holders=1)
    b.acquire("task", "w1", 60)
    assert b.acquire("task", "w2", 60) is False
    b.release("task", "w1")
    assert b.acquire("task", "w2", 60) is True


def test_multi_holder_cap(memory):
    b = CappedBackend(memory, max_holders=3)
    for i in range(3):
        assert b.acquire("task", f"w{i}", 60) is True
    assert b.acquire("task", "w3", 60) is False
    b.release("task", "w0")
    assert b.acquire("task", "w3", 60) is True


def test_capped_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    b = CappedBackend(ns, max_holders=1)
    assert b.acquire("deploy", "ci", 120) is True
    assert b.acquire("deploy", "cd", 120) is False
    b.release("deploy", "ci")
    assert b.acquire("deploy", "cd", 120) is True
