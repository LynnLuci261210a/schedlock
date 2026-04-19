import pytest
from unittest.mock import MagicMock, patch
from schedlock.backends.base import BaseBackend
from schedlock.backends.staggered_backend import StaggeredBackend


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
    return StaggeredBackend(inner, max_delay=0.5)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        StaggeredBackend(inner="not-a-backend")


def test_requires_positive_max_delay(inner):
    with pytest.raises(ValueError):
        StaggeredBackend(inner, max_delay=0)
    with pytest.raises(ValueError):
        StaggeredBackend(inner, max_delay=-1.0)


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_max_delay_property(backend):
    assert backend.max_delay == 0.5


def test_acquire_delegates_to_inner(backend, inner):
    with patch("schedlock.backends.staggered_backend.time.sleep"):
        result = backend.acquire("job:key", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job:key", "worker-1", 30)


def test_acquire_sleeps_random_delay(backend, inner):
    with patch("schedlock.backends.staggered_backend.time.sleep") as mock_sleep, \
         patch("schedlock.backends.staggered_backend.random.uniform", return_value=0.3):
        backend.acquire("key", "owner", 10)
    mock_sleep.assert_called_once_with(0.3)


def test_acquire_delay_bounded_by_max(inner):
    backend = StaggeredBackend(inner, max_delay=2.0)
    delays = []
    original_sleep = __import__("time").sleep
    with patch("schedlock.backends.staggered_backend.time.sleep", side_effect=lambda d: delays.append(d)):
        for _ in range(20):
            backend.acquire("k", "o", 5)
    assert all(0 <= d <= 2.0 for d in delays)


def test_release_delegates(backend, inner):
    result = backend.release("job:key", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job:key", "worker-1")


def test_is_locked_delegates(backend, inner):
    result = backend.is_locked("job:key")
    assert result is False
    inner.is_locked.assert_called_once_with("job:key")


def test_refresh_delegates(backend, inner):
    result = backend.refresh("job:key", "worker-1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job:key", "worker-1", 60)


def test_acquire_returns_false_when_inner_fails(inner):
    inner.acquire.return_value = False
    backend = StaggeredBackend(inner, max_delay=0.1)
    with patch("schedlock.backends.staggered_backend.time.sleep"):
        result = backend.acquire("key", "owner", 10)
    assert result is False
