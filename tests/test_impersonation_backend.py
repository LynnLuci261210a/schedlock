import pytest
from unittest.mock import MagicMock
from schedlock.backends.base import BaseBackend
from schedlock.backends.impersonation_backend import ImpersonationBackend


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
    return ImpersonationBackend(inner, aliases={"alice": "bob"})


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        ImpersonationBackend(object())


def test_aliases_must_be_dict():
    inner = MagicMock(spec=BaseBackend)
    with pytest.raises(TypeError, match="aliases must be a dict or None"):
        ImpersonationBackend(inner, aliases="bad")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_aliases_returns_copy(backend):
    a = backend.aliases
    a["x"] = "y"
    assert "x" not in backend.aliases


def test_acquire_uses_resolved_owner(inner, backend):
    backend.acquire("job", "alice", 60)
    inner.acquire.assert_called_once_with("job", "bob", 60)


def test_acquire_passthrough_for_unknown_owner(inner, backend):
    backend.acquire("job", "charlie", 60)
    inner.acquire.assert_called_once_with("job", "charlie", 60)


def test_release_uses_resolved_owner(inner, backend):
    backend.release("job", "alice")
    inner.release.assert_called_once_with("job", "bob")


def test_release_passthrough_for_unknown_owner(inner, backend):
    backend.release("job", "dave")
    inner.release.assert_called_once_with("job", "dave")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_uses_resolved_owner(inner, backend):
    backend.refresh("job", "alice", 30)
    inner.refresh.assert_called_once_with("job", "bob", 30)


def test_add_alias_registers_mapping(inner):
    b = ImpersonationBackend(inner)
    b.add_alias("eve", "frank")
    b.acquire("job", "eve", 10)
    inner.acquire.assert_called_once_with("job", "frank", 10)


def test_add_alias_rejects_empty_strings(inner):
    b = ImpersonationBackend(inner)
    with pytest.raises(ValueError):
        b.add_alias("", "frank")
    with pytest.raises(ValueError):
        b.add_alias("eve", "")


def test_remove_alias_stops_impersonation(inner):
    b = ImpersonationBackend(inner, aliases={"alice": "bob"})
    b.remove_alias("alice")
    b.acquire("job", "alice", 10)
    inner.acquire.assert_called_once_with("job", "alice", 10)


def test_remove_alias_noop_for_unknown(inner, backend):
    backend.remove_alias("nobody")  # should not raise
