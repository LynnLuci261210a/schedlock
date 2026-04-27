"""Integration tests for SemaphoreBackend over MemoryBackend."""
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.semaphore_backend import SemaphoreBackend


@pytest.fixture()
def memory():
    return MemoryBackend()


def test_full_lifecycle_single_holder(memory):
    sem = SemaphoreBackend(memory, max_holders=1)
    assert sem.acquire("cron:report", "worker-1", 30) is True
    assert sem.is_locked("cron:report") is True
    assert sem.release("cron:report", "worker-1") is True
    assert sem.is_locked("cron:report") is False


def test_second_worker_unblocked_after_release(memory):
    sem = SemaphoreBackend(memory, max_holders=1)
    sem.acquire("cron:report", "worker-1", 30)
    assert sem.acquire("cron:report", "worker-2", 30) is False
    sem.release("cron:report", "worker-1")
    assert sem.acquire("cron:report", "worker-2", 30) is True


def test_multi_holder_allows_concurrent_workers(memory):
    sem = SemaphoreBackend(memory, max_holders=3)
    assert sem.acquire("cron:batch", "w1", 60) is True
    assert sem.acquire("cron:batch", "w2", 60) is True
    assert sem.acquire("cron:batch", "w3", 60) is True
    assert sem.acquire("cron:batch", "w4", 60) is False
    assert sem.current_holders("cron:batch") == {"w1", "w2", "w3"}


def test_semaphore_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    sem = SemaphoreBackend(ns, max_holders=2)
    assert sem.acquire("job", "w1", 30) is True
    assert sem.acquire("job", "w2", 30) is True
    assert sem.acquire("job", "w3", 30) is False
    sem.release("job", "w1")
    assert sem.acquire("job", "w3", 30) is True


def test_slots_available_zero_after_filling(memory):
    sem = SemaphoreBackend(memory, max_holders=2)
    sem.acquire("job", "w1", 30)
    sem.acquire("job", "w2", 30)
    assert sem.slots_available("job") == 0
