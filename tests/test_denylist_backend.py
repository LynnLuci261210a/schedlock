import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.denylist_backend import DenylistBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return DenylistBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        DenylistBackend("not-a-backend")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_allowed_owner_delegates(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_globally_denied_owner_blocked(inner, backend):
    backend.deny("bad-worker")
    result = backend.acquire("job", "bad-worker", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_per_key_denied_owner_blocked(inner, backend):
    backend.deny("worker-2", key="restricted-job")
    result = backend.acquire("restricted-job", "worker-2", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_per_key_denied_does_not_affect_other_keys(inner, backend):
    inner.acquire.return_value = True
    backend.deny("worker-2", key="restricted-job")
    result = backend.acquire("other-job", "worker-2", 30)
    assert result is True
    inner.acquire.assert_called_once_with("other-job", "worker-2", 30)


def test_release_denied_owner_blocked(inner, backend):
    backend.deny("bad-worker")
    result = backend.release("job", "bad-worker")
    assert result is False
    inner.release.assert_not_called()


def test_release_allowed_owner_delegates(inner, backend):
    inner.release.return_value = True
    result = backend.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_allow_removes_global_deny(inner, backend):
    inner.acquire.return_value = True
    backend.deny("worker-3")
    backend.allow("worker-3")
    result = backend.acquire("job", "worker-3", 30)
    assert result is True


def test_allow_removes_per_key_deny(inner, backend):
    inner.acquire.return_value = True
    backend.deny("worker-4", key="job")
    backend.allow("worker-4", key="job")
    result = backend.acquire("job", "worker-4", 30)
    assert result is True


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_denied_owner_blocked(inner, backend):
    backend.deny("bad-worker")
    result = backend.refresh("job", "bad-worker", 60)
    assert result is False
    inner.refresh.assert_not_called()


def test_refresh_allowed_owner_delegates(inner, backend):
    inner.refresh.return_value = True
    result = backend.refresh("job", "worker-1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_deny_requires_non_empty_owner(backend):
    with pytest.raises(ValueError):
        backend.deny("")


def test_global_denied_init_kwarg():
    mem = MemoryBackend()
    b = DenylistBackend(mem, global_denied={"evildoer"})
    assert b.acquire("job", "evildoer", 30) is False
    assert b.acquire("job", "good-worker", 30) is True
