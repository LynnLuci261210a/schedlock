import pytest
from unittest.mock import MagicMock
from schedlock.backends.metrics_backend import MetricsBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture
def backend(inner):
    return MetricsBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        MetricsBackend("not-a-backend")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_success_increments_counters(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("job", "owner1", 60)
    stats = backend.stats()
    assert stats["acquire_attempts"] == 1
    assert stats["acquire_successes"] == 1
    assert stats["acquire_failures"] == 0


def test_acquire_failure_increments_failure_counter(backend, inner):
    inner.acquire.return_value = False
    backend.acquire("job", "owner1", 60)
    stats = backend.stats()
    assert stats["acquire_attempts"] == 1
    assert stats["acquire_successes"] == 0
    assert stats["acquire_failures"] == 1


def test_release_success_increments_counters(backend, inner):
    inner.release.return_value = True
    backend.release("job", "owner1")
    stats = backend.stats()
    assert stats["release_attempts"] == 1
    assert stats["release_successes"] == 1


def test_release_failure_does_not_increment_success(backend, inner):
    inner.release.return_value = False
    backend.release("job", "owner1")
    stats = backend.stats()
    assert stats["release_attempts"] == 1
    assert stats["release_successes"] == 0


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(backend, inner):
    inner.refresh.return_value = True
    assert backend.refresh("job", "owner1", 30) is True
    inner.refresh.assert_called_once_with("job", "owner1", 30)


def test_reset_stats_clears_counters(backend, inner):
    backend.acquire("job", "owner1", 60)
    backend.release("job", "owner1")
    backend.reset_stats()
    stats = backend.stats()
    assert all(v == 0 for v in stats.values())


def test_multiple_operations_accumulate(backend, inner):
    inner.acquire.side_effect = [True, False, True]
    for _ in range(3):
        backend.acquire("job", "owner1", 60)
    stats = backend.stats()
    assert stats["acquire_attempts"] == 3
    assert stats["acquire_successes"] == 2
    assert stats["acquire_failures"] == 1
