"""Integration tests: WeightBackend over MemoryBackend (and NamespacedBackend)."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.weight_backend import WeightBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle_with_high_weight(memory):
    b = WeightBackend(memory, weight_fn=lambda k, o: 9.0, min_weight=5.0)
    assert b.acquire("daily-report", "worker-1", ttl=60) is True
    assert b.is_locked("daily-report") is True
    assert b.release("daily-report", "worker-1") is True
    assert b.is_locked("daily-report") is False


def test_low_weight_prevents_acquire(memory):
    b = WeightBackend(memory, weight_fn=lambda k, o: 1.0, min_weight=5.0)
    assert b.acquire("daily-report", "worker-1") is False
    assert memory.is_locked("daily-report") is False


def test_second_worker_blocked_while_first_holds(memory):
    b = WeightBackend(memory, weight_fn=lambda k, o: 10.0, min_weight=1.0)
    assert b.acquire("sync-job", "worker-1") is True
    assert b.acquire("sync-job", "worker-2") is False


def test_reacquire_after_release(memory):
    b = WeightBackend(memory, weight_fn=lambda k, o: 10.0, min_weight=1.0)
    b.acquire("sync-job", "worker-1")
    b.release("sync-job", "worker-1")
    assert b.acquire("sync-job", "worker-2") is True


def test_weight_backend_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    b = WeightBackend(ns, weight_fn=lambda k, o: 8.0, min_weight=3.0)
    assert b.acquire("cleanup", "w1") is True
    assert b.is_locked("cleanup") is True
    assert b.release("cleanup", "w1") is True
    assert b.is_locked("cleanup") is False


def test_independent_keys_tracked_separately(memory):
    weights = {"job-a": 10.0, "job-b": 0.5}
    b = WeightBackend(memory, weight_fn=lambda k, o: weights[k], min_weight=1.0)
    assert b.acquire("job-a", "w") is True
    assert b.acquire("job-b", "w") is False
    assert b.last_weight("job-a") == pytest.approx(10.0)
    assert b.last_weight("job-b") == pytest.approx(0.5)
