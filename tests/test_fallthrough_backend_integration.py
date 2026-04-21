"""Integration tests for FallthroughBackend over real MemoryBackend instances."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.fallthrough_backend import FallthroughBackend


@pytest.fixture()
def primary():
    return MemoryBackend()


@pytest.fixture()
def secondary():
    return MemoryBackend()


@pytest.fixture()
def backend(primary, secondary):
    return FallthroughBackend([primary, secondary])


def test_full_lifecycle_via_primary(backend, primary, secondary):
    assert backend.acquire("job", "w1", 60) is True
    assert primary.is_locked("job") is True
    assert secondary.is_locked("job") is False
    assert backend.release("job", "w1") is True
    assert primary.is_locked("job") is False


def test_falls_through_when_primary_holds_lock(backend, primary, secondary):
    # Pre-occupy primary with a different owner
    primary.acquire("job", "blocker", 60)
    # FallthroughBackend should fall through to secondary
    assert backend.acquire("job", "w1", 60) is True
    assert secondary.is_locked("job") is True
    assert backend.release("job", "w1") is True
    assert secondary.is_locked("job") is False


def test_all_blocked_returns_false(backend, primary, secondary):
    primary.acquire("job", "blocker", 60)
    secondary.acquire("job", "blocker", 60)
    assert backend.acquire("job", "w1", 60) is False


def test_reacquire_after_release(backend):
    backend.acquire("job", "w1", 60)
    backend.release("job", "w1")
    assert backend.acquire("job", "w2", 60) is True
    assert backend.release("job", "w2") is True


def test_fallthrough_over_namespaced():
    b1 = NamespacedBackend(MemoryBackend(), namespace="ns1")
    b2 = NamespacedBackend(MemoryBackend(), namespace="ns2")
    ft = FallthroughBackend([b1, b2])
    assert ft.acquire("job", "w1", 60) is True
    assert ft.is_locked("job") is True
    assert ft.release("job", "w1") is True
    assert ft.is_locked("job") is False
