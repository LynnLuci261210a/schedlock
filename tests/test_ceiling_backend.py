"""Tests for CeilingBackend."""

from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.ceiling_backend import CeilingBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture()
def inner():
    backend = MagicMock(spec=BaseBackend)
    backend.acquire.return_value = True
    backend.release.return_value = True
    backend.is_locked.return_value = False
    backend.refresh.return_value = True
    return backend


@pytest.fixture()
def backend(inner):
    return CeilingBackend(inner, max_acquires=2)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        CeilingBackend("not-a-backend")


def test_requires_positive_max_acquires(inner):
    with pytest.raises(ValueError, match="max_acquires must be a positive integer"):
        CeilingBackend(inner, max_acquires=0)


def test_requires_integer_max_acquires(inner):
    with pytest.raises(ValueError, match="max_acquires must be a positive integer"):
        CeilingBackend(inner, max_acquires=1.5)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_acquires_property(backend):
    assert backend.max_acquires == 2


def test_initial_count_is_zero(backend):
    assert backend.count_for("my-job") == 0


def test_acquire_delegates_to_inner(inner, backend):
    result = backend.acquire("my-job", "worker-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("my-job", "worker-1", 60)


def test_acquire_increments_count(inner, backend):
    backend.acquire("my-job", "worker-1", 60)
    assert backend.count_for("my-job") == 1


def test_acquire_does_not_increment_on_inner_failure(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("my-job", "worker-1", 60)
    assert backend.count_for("my-job") == 0


def test_acquire_blocked_after_ceiling_reached(inner, backend):
    backend.acquire("my-job", "worker-1", 60)
    backend.acquire("my-job", "worker-2", 60)
    result = backend.acquire("my-job", "worker-3", 60)
    assert result is False
    assert inner.acquire.call_count == 2


def test_count_for_returns_correct_value(inner, backend):
    backend.acquire("my-job", "worker-1", 60)
    assert backend.count_for("my-job") == 1


def test_reset_clears_counter(inner, backend):
    backend.acquire("my-job", "worker-1", 60)
    backend.acquire("my-job", "worker-2", 60)
    backend.reset("my-job")
    assert backend.count_for("my-job") == 0
    result = backend.acquire("my-job", "worker-3", 60)
    assert result is True


def test_independent_keys_tracked_separately(inner, backend):
    backend.acquire("job-a", "worker-1", 60)
    backend.acquire("job-a", "worker-2", 60)
    assert backend.count_for("job-a") == 2
    assert backend.count_for("job-b") == 0
    result = backend.acquire("job-b", "worker-1", 60)
    assert result is True


def test_release_delegates(inner, backend):
    backend.release("my-job", "worker-1")
    inner.release.assert_called_once_with("my-job", "worker-1")


def test_is_locked_delegates(inner, backend):
    backend.is_locked("my-job")
    inner.is_locked.assert_called_once_with("my-job")


def test_refresh_delegates(inner, backend):
    backend.refresh("my-job", "worker-1", 60)
    inner.refresh.assert_called_once_with("my-job", "worker-1", 60)


def test_full_lifecycle_over_memory():
    memory = MemoryBackend()
    ceiling = CeilingBackend(memory, max_acquires=1)
    assert ceiling.acquire("job", "w1", 60) is True
    memory.release("job", "w1")
    assert ceiling.acquire("job", "w2", 60) is False
    ceiling.reset("job")
    assert ceiling.acquire("job", "w2", 60) is True
