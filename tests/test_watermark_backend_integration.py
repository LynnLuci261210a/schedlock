import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.watermark_backend import WatermarkBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_single_owner(memory):
    backend = WatermarkBackend(memory)
    assert backend.acquire("job", "worker-1", 60)
    assert backend.current_for("job") == 1
    assert backend.peak_for("job") == 1
    assert backend.release("job", "worker-1")
    assert backend.current_for("job") == 0
    assert backend.peak_for("job") == 1


def test_peak_reflects_concurrent_holders(memory):
    # Use a capped-style memory backend workaround: acquire different keys
    # to simulate concurrency tracking via watermark
    backend = WatermarkBackend(memory)
    # Simulate multi-holder by manually tracking (memory backend is single-lock)
    # We test watermark logic by mocking multiple successful acquires
    from unittest.mock import MagicMock
    from schedlock.backends.base import BaseBackend
    multi = MagicMock(spec=BaseBackend)
    multi.acquire.return_value = True
    multi.release.return_value = True
    wb = WatermarkBackend(multi)
    wb.acquire("job", "w1", 60)
    wb.acquire("job", "w2", 60)
    wb.acquire("job", "w3", 60)
    assert wb.peak_for("job") == 3
    wb.release("job", "w1")
    wb.release("job", "w2")
    assert wb.current_for("job") == 1
    assert wb.peak_for("job") == 3


def test_independent_keys_tracked_separately(memory):
    backend = WatermarkBackend(memory)
    assert backend.acquire("job-a", "worker-1", 60)
    assert backend.acquire("job-b", "worker-2", 60)
    assert backend.peak_for("job-a") == 1
    assert backend.peak_for("job-b") == 1
    assert backend.current_for("job-a") == 1


def test_watermark_over_namespaced():
    ns = NamespacedBackend(MemoryBackend(), namespace="prod")
    backend = WatermarkBackend(ns)
    assert backend.acquire("job", "worker-1", 60)
    assert backend.peak_for("job") == 1
    assert backend.release("job", "worker-1")
    assert backend.current_for("job") == 0
