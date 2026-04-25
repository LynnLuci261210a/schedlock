import time
import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.leasing_backend import LeasingBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    backend = LeasingBackend(memory, lease_seconds=10.0)
    assert backend.acquire("job", "w1", ttl=30)
    assert backend.is_locked("job")
    assert backend.release("job", "w1")
    assert not backend.is_locked("job")


def test_second_worker_blocked_while_first_holds(memory):
    backend = LeasingBackend(memory, lease_seconds=10.0)
    assert backend.acquire("job", "w1", ttl=30)
    assert not backend.acquire("job", "w2", ttl=30)


def test_second_worker_acquires_after_lease_expiry(memory):
    backend = LeasingBackend(memory, lease_seconds=0.05)
    assert backend.acquire("job", "w1", ttl=1)
    time.sleep(0.1)
    assert backend.acquire("job", "w2", ttl=1)


def test_renew_keeps_lock_alive(memory):
    backend = LeasingBackend(memory, lease_seconds=0.1)
    assert backend.acquire("job", "w1", ttl=30)
    time.sleep(0.08)
    assert backend.renew("job", "w1")
    time.sleep(0.08)
    # after renew the lease should still be valid
    assert backend.is_locked("job")


def test_leasing_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="crons")
    backend = LeasingBackend(ns, lease_seconds=10.0)
    assert backend.acquire("report", "w1", ttl=30)
    assert not backend.acquire("report", "w2", ttl=30)
    assert backend.release("report", "w1")
    assert backend.acquire("report", "w2", ttl=30)


def test_independent_keys_do_not_interfere(memory):
    backend = LeasingBackend(memory, lease_seconds=10.0)
    assert backend.acquire("job-a", "w1", ttl=30)
    assert backend.acquire("job-b", "w2", ttl=30)
    assert not backend.acquire("job-a", "w2", ttl=30)
    assert not backend.acquire("job-b", "w1", ttl=30)
