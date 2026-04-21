"""Unit tests for ExpiringOwnerBackend."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.expiring_owner_backend import ExpiringOwnerBackend


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
    return ExpiringOwnerBackend(inner, expiry_fn=lambda owner: None)


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------

def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        ExpiringOwnerBackend(object(), expiry_fn=lambda o: None)  # type: ignore


def test_requires_callable_expiry_fn(inner):
    with pytest.raises(TypeError, match="expiry_fn must be callable"):
        ExpiringOwnerBackend(inner, expiry_fn="not-callable")  # type: ignore


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

def test_inner_property(inner, backend):
    assert backend.inner is inner


# ---------------------------------------------------------------------------
# acquire behaviour
# ---------------------------------------------------------------------------

def test_acquire_allowed_when_no_expiry(inner, backend):
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 60)


def test_acquire_allowed_before_expiry(inner):
    future = time.time() + 9999
    b = ExpiringOwnerBackend(inner, expiry_fn=lambda o: future)
    assert b.acquire("job", "worker-1", 60) is True
    inner.acquire.assert_called_once()


def test_acquire_blocked_after_expiry(inner):
    past = time.time() - 1
    b = ExpiringOwnerBackend(inner, expiry_fn=lambda o: past)
    assert b.acquire("job", "worker-1", 60) is False
    inner.acquire.assert_not_called()


def test_acquire_blocked_only_for_expired_owner(inner):
    """Different owners can have different expiry times."""
    def expiry_fn(owner: str):
        return time.time() - 1 if owner == "old-worker" else None

    b = ExpiringOwnerBackend(inner, expiry_fn=expiry_fn)
    assert b.acquire("job", "old-worker", 60) is False
    assert b.acquire("job", "new-worker", 60) is True
    inner.acquire.assert_called_once_with("job", "new-worker", 60)


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

def test_release_delegates(inner, backend):
    backend.release("job", "worker-1")
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates(inner, backend):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    backend.refresh("job", "worker-1", 30)
    inner.refresh.assert_called_once_with("job", "worker-1", 30)
