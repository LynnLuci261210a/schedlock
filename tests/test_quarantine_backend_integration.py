import time
import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.quarantine_backend import QuarantineBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_without_quarantine(memory):
    backend = QuarantineBackend(memory, quarantine_seconds=5.0)
    assert backend.acquire("cron:job", "worker-1", 30) is True
    assert backend.is_locked("cron:job") is True
    assert backend.release("cron:job", "worker-1") is True
    assert backend.is_locked("cron:job") is False


def test_quarantined_owner_cannot_acquire(memory):
    backend = QuarantineBackend(memory, quarantine_seconds=5.0)
    backend.quarantine("bad-worker")
    assert backend.acquire("cron:job", "bad-worker", 30) is False
    assert backend.is_locked("cron:job") is False


def test_quarantine_lifts_after_expiry(memory):
    backend = QuarantineBackend(memory, quarantine_seconds=0.05)
    backend.quarantine("worker-temp")
    assert backend.acquire("cron:job", "worker-temp", 30) is False
    time.sleep(0.1)
    assert backend.acquire("cron:job", "worker-temp", 30) is True
    assert backend.is_locked("cron:job") is True


def test_good_worker_unaffected_by_quarantine(memory):
    backend = QuarantineBackend(memory, quarantine_seconds=5.0)
    backend.quarantine("bad-worker")
    assert backend.acquire("cron:job", "good-worker", 30) is True
    assert backend.is_locked("cron:job") is True
    assert backend.release("cron:job", "good-worker") is True


def test_quarantine_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="team-a")
    backend = QuarantineBackend(ns, quarantine_seconds=5.0)
    backend.quarantine("rogue")
    assert backend.acquire("job", "rogue", 30) is False
    assert backend.acquire("job", "trusted", 30) is True
    assert backend.release("job", "trusted") is True
