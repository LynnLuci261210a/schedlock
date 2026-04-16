"""Tests for ReadOnlyBackend."""

import pytest
from unittest.mock import MagicMock
from schedlock.backends.readonly_backend import ReadOnlyBackend


@pytest.fixture
def inner():
    m = MagicMock()
    m.is_locked.return_value = True
    m.get_owner.return_value = "worker-1"
    return m


@pytest.fixture
def backend(inner):
    return ReadOnlyBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(ValueError):
        ReadOnlyBackend(None)


def test_acquire_raises_permission_error(backend):
    with pytest.raises(PermissionError, match="acquire"):
        backend.acquire("my_job", ttl=60, owner="me")


def test_release_raises_permission_error(backend):
    with pytest.raises(PermissionError, match="release"):
        backend.release("my_job", owner="me")


def test_refresh_raises_permission_error(backend):
    with pytest.raises(PermissionError, match="refresh"):
        backend.refresh("my_job", owner="me", ttl=60)


def test_is_locked_delegates_to_inner(backend, inner):
    result = backend.is_locked("my_job")
    assert result is True
    inner.is_locked.assert_called_once_with("my_job")


def test_is_locked_returns_false_when_inner_returns_false(inner):
    inner.is_locked.return_value = False
    b = ReadOnlyBackend(inner)
    assert b.is_locked("my_job") is False


def test_get_owner_delegates_to_inner(backend, inner):
    result = backend.get_owner("my_job")
    assert result == "worker-1"
    inner.get_owner.assert_called_once_with("my_job")


def test_get_owner_returns_none_when_not_supported():
    inner = MagicMock(spec=["is_locked"])
    b = ReadOnlyBackend(inner)
    assert b.get_owner("my_job") is None


def test_does_not_call_inner_acquire_on_error(backend, inner):
    with pytest.raises(PermissionError):
        backend.acquire("my_job", ttl=30)
    inner.acquire.assert_not_called()
