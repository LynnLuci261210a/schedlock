import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.capacity_backend import CapacityBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MemoryBackend()


@pytest.fixture
def backend(inner):
    return CapacityBackend(inner, max_capacity=2)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        CapacityBackend("not-a-backend", max_capacity=2)


def test_requires_positive_max_capacity(inner):
    with pytest.raises(ValueError):
        CapacityBackend(inner, max_capacity=0)
    with pytest.raises(ValueError):
        CapacityBackend(inner, max_capacity=-1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_capacity_property(backend):
    assert backend.max_capacity == 2


def test_current_load_starts_at_zero(backend):
    assert backend.current_load == 0


def test_acquire_succeeds_within_capacity(backend):
    assert backend.acquire("job", "owner-1", 60) is True
    assert backend.current_load == 1


def test_acquire_fills_capacity(backend):
    assert backend.acquire("job-a", "owner-1", 60) is True
    assert backend.acquire("job-b", "owner-2", 60) is True
    assert backend.current_load == 2


def test_acquire_blocked_when_at_capacity(backend):
    backend.acquire("job-a", "owner-1", 60)
    backend.acquire("job-b", "owner-2", 60)
    result = backend.acquire("job-c", "owner-3", 60)
    assert result is False
    assert backend.current_load == 2


def test_release_decrements_load(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.current_load == 1
    backend.release("job", "owner-1")
    assert backend.current_load == 0


def test_acquire_possible_after_release(backend):
    backend.acquire("job-a", "owner-1", 60)
    backend.acquire("job-b", "owner-2", 60)
    backend.release("job-a", "owner-1")
    assert backend.acquire("job-c", "owner-3", 60) is True


def test_failed_inner_acquire_does_not_increment_load():
    mock_inner = MagicMock(spec=BaseBackend)
    mock_inner.acquire.return_value = False
    b = CapacityBackend(mock_inner, max_capacity=5)
    result = b.acquire("job", "owner", 60)
    assert result is False
    assert b.current_load == 0


def test_is_locked_delegates(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.is_locked("job") is True


def test_refresh_delegates(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.refresh("job", "owner-1", 120) is True
