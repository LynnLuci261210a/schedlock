import pytest
from unittest.mock import MagicMock
from schedlock.backends.prefixed_backend import PrefixedBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return PrefixedBackend(inner, "myapp")


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        PrefixedBackend("not-a-backend", "pfx")


def test_requires_non_empty_prefix(inner):
    with pytest.raises(ValueError):
        PrefixedBackend(inner, "")


def test_requires_string_prefix(inner):
    with pytest.raises(ValueError):
        PrefixedBackend(inner, None)


def test_prefix_property(backend):
    assert backend.prefix == "myapp"


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_prepends_prefix(backend, inner):
    inner.acquire.return_value = True
    result = backend.acquire("job1", "owner-1", 30)
    inner.acquire.assert_called_once_with("myapp:job1", "owner-1", 30)
    assert result is True


def test_release_prepends_prefix(backend, inner):
    inner.release.return_value = True
    result = backend.release("job1", "owner-1")
    inner.release.assert_called_once_with("myapp:job1", "owner-1")
    assert result is True


def test_is_locked_prepends_prefix(backend, inner):
    inner.is_locked.return_value = False
    result = backend.is_locked("job1")
    inner.is_locked.assert_called_once_with("myapp:job1")
    assert result is False


def test_refresh_prepends_prefix(backend, inner):
    inner.refresh.return_value = True
    result = backend.refresh("job1", "owner-1", 60)
    inner.refresh.assert_called_once_with("myapp:job1", "owner-1", 60)


def test_get_owner_prepends_prefix(backend, inner):
    inner.get_owner.return_value = "owner-1"
    result = backend.get_owner("job1")
    inner.get_owner.assert_called_once_with("myapp:job1")
    assert result == "owner-1"


def test_integration_with_memory_backend():
    mem = MemoryBackend()
    b1 = PrefixedBackend(mem, "app1")
    b2 = PrefixedBackend(mem, "app2")
    assert b1.acquire("job", "owner", 60) is True
    assert b1.is_locked("job") is True
    assert b2.is_locked("job") is False
    assert b2.acquire("job", "owner", 60) is True
