import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.allowlist_backend import AllowlistBackend


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
    return AllowlistBackend(inner, allowed={"alice", "bob"})


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        AllowlistBackend(object(), allowed={"alice"})


def test_requires_non_empty_allowed(inner):
    with pytest.raises(ValueError):
        AllowlistBackend(inner, allowed=set())


def test_requires_non_empty_string_entries(inner):
    with pytest.raises(ValueError):
        AllowlistBackend(inner, allowed={"", "alice"})


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_allowed_property(backend):
    assert "alice" in backend.allowed
    assert "bob" in backend.allowed
    assert "eve" not in backend.allowed


def test_acquire_allowed_owner(inner, backend):
    result = backend.acquire("job", "alice", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "alice", 60)


def test_acquire_denied_owner(inner, backend):
    with pytest.raises(PermissionError, match="eve"):
        backend.acquire("job", "eve", 60)
    inner.acquire.assert_not_called()


def test_release_allowed_owner(inner, backend):
    result = backend.release("job", "bob")
    assert result is True
    inner.release.assert_called_once_with("job", "bob")


def test_release_denied_owner(inner, backend):
    with pytest.raises(PermissionError, match="mallory"):
        backend.release("job", "mallory")
    inner.release.assert_not_called()


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_allowed_owner(inner, backend):
    result = backend.refresh("job", "alice", 120)
    assert result is True
    inner.refresh.assert_called_once_with("job", "alice", 120)


def test_refresh_denied_owner(inner, backend):
    with pytest.raises(PermissionError):
        backend.refresh("job", "hacker", 120)
    inner.refresh.assert_not_called()
