import pytest
from unittest.mock import MagicMock
from schedlock.backends.namespaced_backend import NamespacedBackend
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
    return NamespacedBackend(inner, namespace="myapp")


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        NamespacedBackend("bad", "ns")


def test_requires_non_empty_namespace(inner):
    with pytest.raises(ValueError):
        NamespacedBackend(inner, "")
    with pytest.raises(ValueError):
        NamespacedBackend(inner, "   ")


def test_namespace_property(backend):
    assert backend.namespace == "myapp"


def test_acquire_prepends_namespace(backend, inner):
    backend.acquire("job", "owner-1", 60)
    inner.acquire.assert_called_once_with("myapp:job", "owner-1", 60)


def test_release_prepends_namespace(backend, inner):
    backend.release("job", "owner-1")
    inner.release.assert_called_once_with("myapp:job", "owner-1")


def test_is_locked_prepends_namespace(backend, inner):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("myapp:job")


def test_refresh_prepends_namespace(backend, inner):
    backend.refresh("job", "owner-1", 30)
    inner.refresh.assert_called_once_with("myapp:job", "owner-1", 30)


def test_acquire_returns_inner_result(inner):
    inner.acquire.return_value = False
    b = NamespacedBackend(inner, "ns")
    assert b.acquire("job", "owner", 60) is False


def test_namespace_strips_whitespace(inner):
    b = NamespacedBackend(inner, "  svc  ")
    assert b.namespace == "svc"
    b.acquire("task", "owner", 10)
    inner.acquire.assert_called_once_with("svc:task", "owner", 10)
