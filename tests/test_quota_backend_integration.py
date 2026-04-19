import time
import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.quota_backend import QuotaBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    b = QuotaBackend(memory, max_acquires=3, window=60.0)
    assert b.acquire("job", "w1", 10) is True
    assert b.is_locked("job") is True
    assert b.release("job", "w1") is True
    assert b.is_locked("job") is False


def test_quota_resets_after_window(memory):
    b = QuotaBackend(memory, max_acquires=2, window=0.05)
    b.acquire("job", "w1", 1)
    b.release("job", "w1")
    b.acquire("job", "w1", 1)
    b.release("job", "w1")
    assert b.acquire("job", "w1", 1) is False
    time.sleep(0.1)
    assert b.acquire("job", "w1", 1) is True


def test_quota_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    b = QuotaBackend(ns, max_acquires=1, window=60.0)
    assert b.acquire("job", "w1", 10) is True
    b.release("job", "w1")
    assert b.acquire("job", "w1", 10) is False


def test_multiple_workers_share_quota(memory):
    b = QuotaBackend(memory, max_acquires=2, window=60.0)
    b.acquire("job", "w1", 1)
    b.release("job", "w1")
    b.acquire("job", "w2", 1)
    b.release("job", "w2")
    # quota exhausted regardless of owner
    assert b.acquire("job", "w3", 1) is False
