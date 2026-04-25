"""Tests for TokenRelayBackend."""
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.token_relay_backend import TokenRelayBackend


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
    return TokenRelayBackend(inner, token_fn=lambda: "relay-token")


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        TokenRelayBackend(object(), token_fn=lambda: "tok")


def test_requires_callable_token_fn(inner):
    with pytest.raises(TypeError, match="token_fn must be callable"):
        TokenRelayBackend(inner, token_fn="not-callable")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_token_fn_property(backend):
    assert callable(backend.token_fn)


def test_acquire_uses_resolved_token(inner, backend):
    result = backend.acquire("my-job", "ignored-owner", 30)
    assert result is True
    inner.acquire.assert_called_once_with("my-job", "relay-token", 30)


def test_acquire_ignores_caller_owner(inner):
    backend = TokenRelayBackend(inner, token_fn=lambda: "computed-owner")
    backend.acquire("k", "original", 10)
    inner.acquire.assert_called_once_with("k", "computed-owner", 10)


def test_release_uses_resolved_token(inner, backend):
    result = backend.release("my-job", "ignored-owner")
    assert result is True
    inner.release.assert_called_once_with("my-job", "relay-token")


def test_is_locked_delegates_unchanged(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("my-job") is True
    inner.is_locked.assert_called_once_with("my-job")


def test_refresh_uses_resolved_token(inner, backend):
    result = backend.refresh("my-job", "ignored", 60)
    assert result is True
    inner.refresh.assert_called_once_with("my-job", "relay-token", 60)


def test_token_fn_called_on_every_acquire(inner):
    calls = []

    def rotating_token():
        calls.append(1)
        return f"token-{len(calls)}"

    backend = TokenRelayBackend(inner, token_fn=rotating_token)
    backend.acquire("k", "x", 5)
    backend.acquire("k", "x", 5)
    assert len(calls) == 2
    assert inner.acquire.call_args_list[0][0][1] == "token-1"
    assert inner.acquire.call_args_list[1][0][1] == "token-2"


def test_acquire_raises_if_token_fn_returns_empty(inner):
    backend = TokenRelayBackend(inner, token_fn=lambda: "")
    with pytest.raises(ValueError, match="non-empty string"):
        backend.acquire("k", "o", 10)


def test_release_raises_if_token_fn_returns_non_string(inner):
    backend = TokenRelayBackend(inner, token_fn=lambda: None)
    with pytest.raises(ValueError, match="non-empty string"):
        backend.release("k", "o")
