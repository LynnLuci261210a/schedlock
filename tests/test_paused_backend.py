"""Unit tests for PausedBackend."""

from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.paused_backend import PausedBackend


@pytest.fixture()
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture()
def backend(inner):
    return PausedBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        PausedBackend("not-a-backend")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_is_paused_initially_false(backend):
    assert backend.is_paused is False


def test_is_paused_initially_true_when_set(inner):
    b = PausedBackend(inner, paused=True)
    assert b.is_paused is True


def test_acquire_delegates_when_not_paused(backend, inner):
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_blocked_when_paused(backend, inner):
    backend.pause()
    result = backend.acquire("job", "worker-1", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_pause_sets_is_paused(backend):
    backend.pause()
    assert backend.is_paused is True


def test_resume_clears_is_paused(backend):
    backend.pause()
    backend.resume()
    assert backend.is_paused is False


def test_acquire_resumes_after_resume(backend, inner):
    backend.pause()
    backend.resume()
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_release_always_delegates(backend, inner):
    backend.pause()
    result = backend.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_always_delegates(backend, inner):
    backend.pause()
    result = backend.is_locked("job")
    assert result is False
    inner.is_locked.assert_called_once_with("job")


def test_refresh_always_delegates(backend, inner):
    backend.pause()
    result = backend.refresh("job", "worker-1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_is_base_subclass():
    assert issubclass(PausedBackend, BaseBackend)
