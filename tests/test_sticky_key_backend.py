import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.sticky_key_backend import StickyKeyBackend


@pytest.fixture
def inner():
    return MemoryBackend()


@pytest.fixture
def backend(inner):
    return StickyKeyBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        StickyKeyBackend("not-a-backend")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_success_binds_owner(backend):
    assert backend.acquire("job", "worker-1", 60) is True
    assert backend.bound_owner("job") == "worker-1"


def test_same_owner_can_reacquire(backend):
    backend.acquire("job", "worker-1", 60)
    # release so inner lock is free, then re-acquire
    backend.release("job", "worker-1")
    assert backend.acquire("job", "worker-1", 60) is True
    assert backend.bound_owner("job") == "worker-1"


def test_different_owner_blocked_while_bound(backend):
    backend.acquire("job", "worker-1", 60)
    assert backend.acquire("job", "worker-2", 60) is False


def test_release_clears_binding(backend):
    backend.acquire("job", "worker-1", 60)
    backend.release("job", "worker-1")
    assert backend.bound_owner("job") is None


def test_new_owner_can_acquire_after_release(backend):
    backend.acquire("job", "worker-1", 60)
    backend.release("job", "worker-1")
    assert backend.acquire("job", "worker-2", 60) is True
    assert backend.bound_owner("job") == "worker-2"


def test_failed_acquire_does_not_bind(backend):
    # inner already holds the lock for worker-1
    backend.inner.acquire("job", "worker-1", 60)
    result = backend.acquire("job", "worker-2", 60)
    assert result is False
    assert backend.bound_owner("job") is None


def test_release_returns_false_for_wrong_owner(backend):
    backend.acquire("job", "worker-1", 60)
    result = backend.release("job", "worker-2")
    assert result is False
    # binding should still be intact
    assert backend.bound_owner("job") == "worker-1"


def test_clear_binding_removes_without_releasing(backend):
    backend.acquire("job", "worker-1", 60)
    backend.clear_binding("job")
    assert backend.bound_owner("job") is None
    # underlying lock still held
    assert backend.inner.is_locked("job") is True


def test_is_locked_delegates_to_inner(backend):
    assert backend.is_locked("job") is False
    backend.acquire("job", "worker-1", 60)
    assert backend.is_locked("job") is True


def test_refresh_delegates_to_inner(backend):
    backend.acquire("job", "worker-1", 60)
    assert backend.refresh("job", "worker-1", 120) is True


def test_independent_keys_have_independent_bindings(backend):
    backend.acquire("job-a", "worker-1", 60)
    backend.acquire("job-b", "worker-2", 60)
    assert backend.bound_owner("job-a") == "worker-1"
    assert backend.bound_owner("job-b") == "worker-2"
    # worker-2 cannot acquire job-a (bound to worker-1)
    assert backend.acquire("job-a", "worker-2", 60) is False
