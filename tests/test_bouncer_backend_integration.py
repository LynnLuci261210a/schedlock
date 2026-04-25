"""Integration tests for BouncerBackend over MemoryBackend."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.bouncer_backend import BouncerBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_allowed_owner_full_lifecycle(memory):
    allowed = {"worker-1"}
    b = BouncerBackend(memory, bouncer=lambda k, o: o in allowed)

    assert b.acquire("job", "worker-1", 30) is True
    assert b.is_locked("job") is True
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False


def test_denied_owner_cannot_acquire(memory):
    allowed = {"worker-1"}
    b = BouncerBackend(memory, bouncer=lambda k, o: o in allowed)

    assert b.acquire("job", "intruder", 30) is False
    assert b.is_locked("job") is False


def test_denied_owner_cannot_steal_held_lock(memory):
    allowed = {"worker-1"}
    b = BouncerBackend(memory, bouncer=lambda k, o: o in allowed)

    b.acquire("job", "worker-1", 30)
    # intruder is blocked by bouncer before even reaching inner
    assert b.acquire("job", "intruder", 30) is False
    # lock still held by worker-1
    assert memory.is_locked("job") is True


def test_second_allowed_worker_blocked_while_first_holds(memory):
    """Both workers are allowed by the bouncer, but the lock is held."""
    b = BouncerBackend(memory, bouncer=lambda k, o: True)

    assert b.acquire("job", "worker-1", 30) is True
    assert b.acquire("job", "worker-2", 30) is False


def test_bouncer_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    blocked_keys = {"restricted"}
    b = BouncerBackend(ns, bouncer=lambda k, o: k not in blocked_keys)

    assert b.acquire("open", "worker-1", 30) is True
    assert b.acquire("restricted", "worker-1", 30) is False
    assert b.is_locked("open") is True
    assert b.is_locked("restricted") is False
