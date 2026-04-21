"""Tests for StickyOwnerBackend."""

import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.sticky_owner_backend import StickyOwnerBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return StickyOwnerBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        StickyOwnerBackend("not-a-backend")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_success_binds_owner(backend, inner):
    inner.acquire.return_value = True
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    assert backend.bound_owner("job") == "worker-1"


def test_acquire_failure_does_not_bind(backend, inner):
    inner.acquire.return_value = False
    result = backend.acquire("job", "worker-1", 60)
    assert result is False
    assert backend.bound_owner("job") is None


def test_second_owner_blocked_when_first_bound(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("job", "worker-1", 60)
    # worker-2 should be rejected without even calling inner
    result = backend.acquire("job", "worker-2", 60)
    assert result is False
    # inner.acquire should only have been called once (for worker-1)
    assert inner.acquire.call_count == 1


def test_same_owner_can_reacquire(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("job", "worker-1", 60)
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    assert inner.acquire.call_count == 2


def test_release_clears_binding(backend, inner):
    inner.acquire.return_value = True
    inner.release.return_value = True
    backend.acquire("job", "worker-1", 60)
    backend.release("job", "worker-1")
    assert backend.bound_owner("job") is None


def test_release_wrong_owner_does_not_clear_binding(backend, inner):
    inner.acquire.return_value = True
    inner.release.return_value = False
    backend.acquire("job", "worker-1", 60)
    backend.release("job", "worker-2")
    assert backend.bound_owner("job") == "worker-1"


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(backend, inner):
    inner.refresh.return_value = True
    assert backend.refresh("job", "worker-1", 30) is True
    inner.refresh.assert_called_once_with("job", "worker-1", 30)


def test_bound_owner_none_for_unknown_key(backend):
    assert backend.bound_owner("unknown") is None


def test_full_lifecycle_over_memory():
    """Integration: sticky owner over a real MemoryBackend."""
    mem = MemoryBackend()
    b = StickyOwnerBackend(mem)

    assert b.acquire("job", "worker-1", 60) is True
    assert b.acquire("job", "worker-2", 60) is False
    assert b.bound_owner("job") == "worker-1"

    assert b.release("job", "worker-1") is True
    assert b.bound_owner("job") is None

    # After release, worker-2 can now acquire
    assert b.acquire("job", "worker-2", 60) is True
    assert b.bound_owner("job") == "worker-2"
