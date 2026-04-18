import pytest
from unittest.mock import MagicMock, patch
from schedlock.backends.jitter_backend import JitterBackend
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
    return JitterBackend(inner, max_jitter=0.5)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        JitterBackend("not-a-backend")


def test_requires_positive_max_jitter(inner):
    with pytest.raises(ValueError):
        JitterBackend(inner, max_jitter=0)
    with pytest.raises(ValueError):
        JitterBackend(inner, max_jitter=-1.0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_jitter_property(backend):
    assert backend.max_jitter == 0.5


def test_acquire_delegates_to_inner(inner, backend):
    with patch("schedlock.backends.jitter_backend.time.sleep"):
        result = backend.acquire("my-job", "worker-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("my-job", "worker-1", 60)


def test_acquire_sleeps_within_jitter_range(inner, backend):
    with patch("schedlock.backends.jitter_backend.time.sleep") as mock_sleep:
        with patch("schedlock.backends.jitter_backend.random.uniform", return_value=0.3):
            backend.acquire("key", "owner", 30)
    mock_sleep.assert_called_once_with(0.3)


def test_acquire_returns_false_when_inner_fails(inner, backend):
    inner.acquire.return_value = False
    with patch("schedlock.backends.jitter_backend.time.sleep"):
        result = backend.acquire("key", "owner", 30)
    assert result is False


def test_release_delegates_to_inner(inner, backend):
    result = backend.release("my-job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("my-job", "worker-1")


def test_is_locked_delegates_to_inner(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("my-job") is True
    inner.is_locked.assert_called_once_with("my-job")


def test_refresh_delegates_to_inner(inner, backend):
    result = backend.refresh("my-job", "worker-1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("my-job", "worker-1", 60)
