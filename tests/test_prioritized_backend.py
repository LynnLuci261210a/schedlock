import pytest
from unittest.mock import MagicMock
from schedlock.backends.prioritized_backend import PrioritizedBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return PrioritizedBackend(inner, min_priority=5)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        PrioritizedBackend("not-a-backend")


def test_requires_non_negative_min_priority(inner):
    with pytest.raises(ValueError):
        PrioritizedBackend(inner, min_priority=-1)


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_min_priority_property(backend):
    assert backend.min_priority == 5


def test_acquire_blocked_when_priority_too_low(backend, inner):
    result = backend.acquire("job", "owner", 30, priority=3)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_allowed_at_exact_min_priority(backend, inner):
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner", 30, priority=5)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner", 30)


def test_acquire_allowed_above_min_priority(backend, inner):
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner", 30, priority=10)
    assert result is True


def test_acquire_default_priority_zero_with_zero_min(inner):
    b = PrioritizedBackend(inner, min_priority=0)
    inner.acquire.return_value = True
    assert b.acquire("job", "owner", 30) is True


def test_acquire_invalid_priority_raises(backend):
    with pytest.raises(ValueError):
        backend.acquire("job", "owner", 30, priority=-1)


def test_release_delegates(backend, inner):
    inner.release.return_value = True
    assert backend.release("job", "owner") is True
    inner.release.assert_called_once_with("job", "owner")


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = False
    assert backend.is_locked("job") is False


def test_integration_with_memory_backend():
    memory = MemoryBackend()
    b = PrioritizedBackend(memory, min_priority=3)
    assert b.acquire("job", "o1", 60, priority=2) is False
    assert b.acquire("job", "o1", 60, priority=3) is True
    assert b.is_locked("job") is True
    assert b.release("job", "o1") is True
    assert b.is_locked("job") is False
