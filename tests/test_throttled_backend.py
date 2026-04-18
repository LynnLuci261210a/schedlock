import time
import pytest
from unittest.mock import MagicMock
from schedlock.backends.throttled_backend import ThrottledBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return ThrottledBackend(inner, max_acquires=2, window=5.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        ThrottledBackend("not-a-backend", max_acquires=2, window=5.0)


def test_requires_positive_max_acquires(inner):
    with pytest.raises(ValueError):
        ThrottledBackend(inner, max_acquires=0, window=5.0)


def test_requires_positive_window(inner):
    with pytest.raises(ValueError):
        ThrottledBackend(inner, max_acquires=2, window=0)


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_max_acquires_property(backend):
    assert backend.max_acquires == 2


def test_window_property(backend):
    assert backend.window == 5.0


def test_acquire_delegates_when_not_throttled(backend, inner):
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner1", 30)


def test_acquire_blocked_after_max_acquires(inner):
    backend = ThrottledBackend(inner, max_acquires=2, window=10.0)
    inner.acquire.return_value = True
    assert backend.acquire("job", "o1", 30) is True
    assert backend.acquire("job", "o2", 30) is True
    assert backend.acquire("job", "o3", 30) is False
    assert inner.acquire.call_count == 2


def test_acquire_not_recorded_on_inner_failure(inner):
    backend = ThrottledBackend(inner, max_acquires=1, window=10.0)
    inner.acquire.return_value = False
    assert backend.acquire("job", "o1", 30) is False
    assert backend.acquire("job", "o2", 30) is False
    assert inner.acquire.call_count == 2


def test_release_delegates(backend, inner):
    inner.release.return_value = True
    assert backend.release("job", "owner1") is True
    inner.release.assert_called_once_with("job", "owner1")


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_window_expiry_allows_reacquire():
    memory = MemoryBackend()
    backend = ThrottledBackend(memory, max_acquires=1, window=0.1)
    assert backend.acquire("job", "o1", 60) is True
    memory.release("job", "o1")
    assert backend.acquire("job", "o2", 60) is False
    time.sleep(0.15)
    assert backend.acquire("job", "o3", 60) is True
