import pytest
from unittest.mock import MagicMock
from schedlock.backends.blacklist_backend import BlacklistBackend
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
    return BlacklistBackend(inner, {"bad-owner", "banned"})


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        BlacklistBackend("not-a-backend", {"x"})


def test_requires_non_empty_blacklist(inner):
    with pytest.raises(ValueError):
        BlacklistBackend(inner, set())


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_blacklisted_property(backend):
    assert "bad-owner" in backend.blacklisted
    assert "banned" in backend.blacklisted


def test_acquire_allowed_owner_delegates(inner, backend):
    result = backend.acquire("job", "good-owner", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "good-owner", 60)


def test_acquire_blacklisted_owner_returns_false(inner, backend):
    result = backend.acquire("job", "bad-owner", 60)
    assert result is False
    inner.acquire.assert_not_called()


def test_release_allowed_owner_delegates(inner, backend):
    result = backend.release("job", "good-owner")
    assert result is True
    inner.release.assert_called_once_with("job", "good-owner")


def test_release_blacklisted_owner_returns_false(inner, backend):
    result = backend.release("job", "banned")
    assert result is False
    inner.release.assert_not_called()


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_allowed_owner_delegates(inner, backend):
    result = backend.refresh("job", "good-owner", 30)
    assert result is True
    inner.refresh.assert_called_once_with("job", "good-owner", 30)


def test_refresh_blacklisted_owner_returns_false(inner, backend):
    result = backend.refresh("job", "bad-owner", 30)
    assert result is False
    inner.refresh.assert_not_called()
