"""Tests for DrainingBackend."""

from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.draining_backend import DrainingBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture()
def inner():
    mock = MagicMock(spec=BaseBackend)
    mock.acquire.return_value = True
    mock.release.return_value = True
    mock.is_locked.return_value = False
    mock.refresh.return_value = True
    return mock


@pytest.fixture()
def backend(inner):
    return DrainingBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        DrainingBackend("not-a-backend")  # type: ignore[arg-type]


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_is_draining_initially_false(backend):
    assert backend.is_draining is False


def test_drain_sets_draining_true(backend):
    backend.drain()
    assert backend.is_draining is True


def test_resume_clears_draining(backend):
    backend.drain()
    backend.resume()
    assert backend.is_draining is False


def test_acquire_delegates_when_not_draining(backend, inner):
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_blocked_when_draining(backend, inner):
    backend.drain()
    result = backend.acquire("job", "worker-1", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_resumes_after_resume_call(backend, inner):
    backend.drain()
    backend.resume()
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_release_delegates_unconditionally(backend, inner):
    backend.drain()
    result = backend.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates(backend, inner):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(backend, inner):
    backend.refresh("job", "worker-1", 60)
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_full_lifecycle_over_memory():
    mem = MemoryBackend()
    draining = DrainingBackend(mem)

    assert draining.acquire("cron:job", "host-1", 30) is True
    assert mem.is_locked("cron:job") is True

    draining.drain()
    assert draining.acquire("cron:job2", "host-2", 30) is False
    assert mem.is_locked("cron:job2") is False

    assert draining.release("cron:job", "host-1") is True
    assert mem.is_locked("cron:job") is False
