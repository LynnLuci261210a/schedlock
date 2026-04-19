import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.expiry_policy_backend import ExpiryPolicyBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_with_policy(memory):
    backend = ExpiryPolicyBackend(memory, policy=lambda k, o: 60)
    assert backend.acquire("job", "w1", ttl=0) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "w1") is True
    assert backend.is_locked("job") is False


def test_policy_overrides_passed_ttl(memory):
    backend = ExpiryPolicyBackend(memory, policy=lambda k, o: 60)
    # ttl=1 passed but policy says 60 — lock should not expire immediately
    backend.acquire("job", "w1", ttl=1)
    # second acquire blocked because policy gave a long TTL
    assert backend.acquire("job", "w2", ttl=1) is False


def test_second_worker_blocked(memory):
    backend = ExpiryPolicyBackend(memory, policy=lambda k, o: 30)
    backend.acquire("job", "w1", ttl=0)
    assert backend.acquire("job", "w2", ttl=0) is False


def test_reacquire_after_release(memory):
    backend = ExpiryPolicyBackend(memory, policy=lambda k, o: 30)
    backend.acquire("job", "w1", ttl=0)
    backend.release("job", "w1")
    assert backend.acquire("job", "w2", ttl=0) is True


def test_policy_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    backend = ExpiryPolicyBackend(ns, policy=lambda k, o: 45)
    assert backend.acquire("report", "w1", ttl=0) is True
    assert backend.is_locked("report") is True
    assert backend.release("report", "w1") is True
    assert backend.is_locked("report") is False
