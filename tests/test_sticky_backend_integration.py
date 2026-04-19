"""Integration tests for StickyBackend over MemoryBackend."""
import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.sticky_backend import StickyBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    backend = StickyBackend(memory)
    assert backend.acquire("job", "w1", 60) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "w1") is True
    assert backend.is_locked("job") is False


def test_second_owner_blocked_while_first_holds(memory):
    backend = StickyBackend(memory)
    backend.acquire("job", "w1", 60)
    assert backend.acquire("job", "w2", 60) is False


def test_second_owner_blocked_even_after_inner_expiry(memory):
    """Sticky binding persists even if inner lock expired (TTL=0 trick)."""
    backend = StickyBackend(memory)
    backend.acquire("job", "w1", 0)  # immediate expiry in memory backend
    # inner is_locked may now return False, but sticky still bound to w1
    assert backend.acquire("job", "w2", 60) is False


def test_reacquire_after_release(memory):
    backend = StickyBackend(memory)
    backend.acquire("job", "w1", 60)
    backend.release("job", "w1")
    assert backend.acquire("job", "w2", 60) is True
    assert backend.sticky_owner("job") == "w2"


def test_sticky_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    backend = StickyBackend(ns)
    assert backend.acquire("job", "w1", 60) is True
    assert backend.acquire("job", "w2", 60) is False
    backend.release("job", "w1")
    assert backend.acquire("job", "w2", 60) is True
