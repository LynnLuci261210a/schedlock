"""Tests for SuspendableBackend."""
import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.suspendable_backend import SuspendableBackend


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
    return SuspendableBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        SuspendableBackend("not-a-backend")


def test_requires_non_empty_reason(inner):
    with pytest.raises(ValueError):
        SuspendableBackend(inner, reason="")
    with pytest.raises(ValueError):
        SuspendableBackend(inner, reason="   ")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_reason_default(backend):
    assert backend.reason == "backend suspended"


def test_reason_custom(inner):
    b = SuspendableBackend(inner, reason="maintenance window")
    assert b.reason == "maintenance window"


def test_is_suspended_initially_false(backend):
    assert backend.is_suspended is False


def test_acquire_delegates_when_active(inner, backend):
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_blocked_when_suspended(inner, backend):
    backend.suspend()
    result = backend.acquire("job", "worker-1", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_suspend_sets_flag(backend):
    backend.suspend()
    assert backend.is_suspended is True


def test_resume_clears_flag(backend):
    backend.suspend()
    backend.resume()
    assert backend.is_suspended is False


def test_acquire_allowed_after_resume(inner, backend):
    backend.suspend()
    backend.resume()
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once()


def test_release_delegates_while_suspended(inner, backend):
    backend.suspend()
    result = backend.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates_while_suspended(inner, backend):
    backend.suspend()
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_blocked_when_suspended(inner, backend):
    backend.suspend()
    result = backend.refresh("job", "worker-1", 30)
    assert result is False
    inner.refresh.assert_not_called()


def test_refresh_delegates_when_active(inner, backend):
    result = backend.refresh("job", "worker-1", 30)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker-1", 30)
