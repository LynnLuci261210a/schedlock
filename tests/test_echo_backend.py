"""Tests for EchoBackend."""
from unittest.mock import MagicMock

import pytest

from schedlock.backends.echo_backend import EchoBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture()
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture()
def messages():
    return []


@pytest.fixture()
def backend(inner, messages):
    return EchoBackend(inner, callback=messages.append)


def test_requires_inner_backend(messages):
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        EchoBackend(object(), callback=messages.append)


def test_requires_callable_callback(inner):
    with pytest.raises(TypeError, match="callback must be callable"):
        EchoBackend(inner, callback="not_callable")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_delegates_to_inner(backend, inner):
    result = backend.acquire("job", "worker-1", 60)
    inner.acquire.assert_called_once_with("job", "worker-1", 60)
    assert result is True


def test_acquire_success_echoes_acquired(backend, messages):
    backend.acquire("job", "worker-1", 60)
    assert len(messages) == 1
    assert "acquire" in messages[0]
    assert "acquired" in messages[0]
    assert "job" in messages[0]


def test_acquire_blocked_echoes_blocked(backend, inner, messages):
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 60)
    assert "blocked" in messages[0]


def test_release_delegates_to_inner(backend, inner):
    result = backend.release("job", "worker-1")
    inner.release.assert_called_once_with("job", "worker-1")
    assert result is True


def test_release_echoes_released(backend, messages):
    backend.release("job", "worker-1")
    assert "release" in messages[0]
    assert "released" in messages[0]


def test_release_not_held_echoes_not_held(backend, inner, messages):
    inner.release.return_value = False
    backend.release("job", "worker-1")
    assert "not_held" in messages[0]


def test_is_locked_delegates_to_inner(backend, inner):
    result = backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")
    assert result is False


def test_is_locked_echoes_result(backend, messages):
    backend.is_locked("job")
    assert "is_locked" in messages[0]
    assert "False" in messages[0]


def test_refresh_delegates_to_inner(backend, inner):
    result = backend.refresh("job", "worker-1", 30)
    inner.refresh.assert_called_once_with("job", "worker-1", 30)
    assert result is True


def test_refresh_echoes_refreshed(backend, messages):
    backend.refresh("job", "worker-1", 30)
    assert "refresh" in messages[0]
    assert "refreshed" in messages[0]


def test_multiple_operations_produce_multiple_messages(backend, messages):
    backend.acquire("job", "w", 10)
    backend.is_locked("job")
    backend.release("job", "w")
    assert len(messages) == 3
