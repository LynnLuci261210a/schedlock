"""Tests for RetryingOwnerBackend."""
from unittest.mock import MagicMock, call

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.retrying_owner_backend import RetryingOwnerBackend


@pytest.fixture
def inner():
    backend = MagicMock(spec=BaseBackend)
    return backend


@pytest.fixture
def backend(inner):
    counter = {"n": 0}

    def factory():
        counter["n"] += 1
        return f"owner-{counter['n']}"

    return RetryingOwnerBackend(inner, factory, retries=2, delay=0)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        RetryingOwnerBackend(object(), lambda: "o", retries=1, delay=0)


def test_requires_callable_owner_factory(inner):
    with pytest.raises(TypeError, match="owner_factory must be callable"):
        RetryingOwnerBackend(inner, "not-callable", retries=1, delay=0)


def test_requires_non_negative_retries(inner):
    with pytest.raises(ValueError, match="retries must be a non-negative integer"):
        RetryingOwnerBackend(inner, lambda: "o", retries=-1, delay=0)


def test_requires_non_negative_delay(inner):
    with pytest.raises(ValueError, match="delay must be a non-negative number"):
        RetryingOwnerBackend(inner, lambda: "o", retries=1, delay=-0.1)


def test_inner_property(inner):
    b = RetryingOwnerBackend(inner, lambda: "o", retries=0, delay=0)
    assert b.inner is inner


def test_retries_property(inner):
    b = RetryingOwnerBackend(inner, lambda: "o", retries=3, delay=0)
    assert b.retries == 3


def test_delay_property(inner):
    b = RetryingOwnerBackend(inner, lambda: "o", retries=1, delay=0.5)
    assert b.delay == 0.5


def test_acquire_succeeds_on_first_attempt(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "ignored", 60)
    assert result is True
    assert inner.acquire.call_count == 1
    # owner comes from factory, not the passed-in value
    args = inner.acquire.call_args[0]
    assert args[1] == "owner-1"


def test_acquire_succeeds_on_second_attempt(inner, backend):
    inner.acquire.side_effect = [False, True]
    result = backend.acquire("job", "ignored", 60)
    assert result is True
    assert inner.acquire.call_count == 2


def test_acquire_fails_after_all_retries(inner, backend):
    inner.acquire.return_value = False
    result = backend.acquire("job", "ignored", 60)
    assert result is False
    assert inner.acquire.call_count == 3  # 1 initial + 2 retries


def test_each_attempt_uses_fresh_owner(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "ignored", 60)
    owners = [c[0][1] for c in inner.acquire.call_args_list]
    assert owners == ["owner-1", "owner-2", "owner-3"]


def test_release_delegates(inner, backend):
    inner.release.return_value = True
    result = backend.release("job", "owner-1")
    assert result is True
    inner.release.assert_called_once_with("job", "owner-1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    assert backend.refresh("job", "owner-1", 30) is True
    inner.refresh.assert_called_once_with("job", "owner-1", 30)
