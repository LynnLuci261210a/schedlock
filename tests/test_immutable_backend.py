"""Tests for ImmutableBackend."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.immutable_backend import ImmutableBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture()
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture()
def backend(inner):
    return ImmutableBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        ImmutableBackend("not-a-backend")  # type: ignore


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_delegates_to_inner(backend, inner):
    inner.acquire.return_value = True
    result = backend.acquire("job", "worker-1", 60)
    inner.acquire.assert_called_once_with("job", "worker-1", 60)
    assert result is True


def test_acquire_returns_false_when_inner_fails(backend, inner):
    inner.acquire.return_value = False
    result = backend.acquire("job", "worker-1", 60)
    assert result is False


def test_release_raises_permission_error(backend):
    with pytest.raises(PermissionError, match="release is not permitted"):
        backend.release("job", "worker-1")


def test_release_error_message_contains_key(backend):
    with pytest.raises(PermissionError, match="my-special-job"):
        backend.release("my-special-job", "worker-1")


def test_release_does_not_call_inner(backend, inner):
    with pytest.raises(PermissionError):
        backend.release("job", "worker-1")
    inner.release.assert_not_called()


def test_is_locked_delegates_to_inner(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates_to_inner(backend, inner):
    inner.refresh.return_value = True
    result = backend.refresh("job", "worker-1", 120)
    inner.refresh.assert_called_once_with("job", "worker-1", 120)
    assert result is True


def test_integration_acquire_then_release_blocked():
    memory = MemoryBackend()
    backend = ImmutableBackend(memory)

    assert backend.acquire("job", "worker-1", 30) is True
    assert backend.is_locked("job") is True

    with pytest.raises(PermissionError):
        backend.release("job", "worker-1")

    # Lock still held because release was blocked
    assert backend.is_locked("job") is True


def test_integration_second_acquire_blocked_while_held():
    memory = MemoryBackend()
    backend = ImmutableBackend(memory)

    assert backend.acquire("job", "worker-1", 30) is True
    assert backend.acquire("job", "worker-2", 30) is False
