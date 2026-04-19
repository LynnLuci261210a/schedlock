import pytest
from unittest.mock import MagicMock
from schedlock.backends.sticky_backend import StickyBackend
from schedlock.backends.base import BaseBackend


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
    return StickyBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        StickyBackend("not-a-backend")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_success_binds_owner(inner, backend):
    assert backend.acquire("job", "worker-1", 30) is True
    assert backend.sticky_owner("job") == "worker-1"


def test_acquire_same_owner_allowed(inner, backend):
    backend.acquire("job", "worker-1", 30)
    inner.acquire.return_value = True
    assert backend.acquire("job", "worker-1", 30) is True


def test_acquire_different_owner_blocked(inner, backend):
    backend.acquire("job", "worker-1", 30)
    result = backend.acquire("job", "worker-2", 30)
    assert result is False
    # inner should NOT have been called for the second attempt
    assert inner.acquire.call_count == 1


def test_acquire_failure_does_not_bind(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 30)
    assert backend.sticky_owner("job") is None


def test_release_clears_sticky_binding(inner, backend):
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    assert backend.sticky_owner("job") is None


def test_release_wrong_owner_does_not_clear(inner, backend):
    backend.acquire("job", "worker-1", 30)
    inner.release.return_value = False
    backend.release("job", "worker-2")
    assert backend.sticky_owner("job") == "worker-1"


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True


def test_refresh_delegates(inner, backend):
    assert backend.refresh("job", "worker-1", 60) is True
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_sticky_owner_unknown_key_returns_none(backend):
    assert backend.sticky_owner("nonexistent") is None


def test_after_release_new_owner_can_acquire(inner, backend):
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    inner.acquire.return_value = True
    assert backend.acquire("job", "worker-2", 30) is True
    assert backend.sticky_owner("job") == "worker-2"
