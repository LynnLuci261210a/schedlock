import pytest
from unittest.mock import MagicMock
from schedlock.backends.validating_backend import ValidatingBackend
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
    return ValidatingBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        ValidatingBackend(inner="not-a-backend")


def test_requires_positive_max_key_length(inner):
    with pytest.raises(ValueError):
        ValidatingBackend(inner, max_key_length=0)


def test_requires_positive_max_owner_length(inner):
    with pytest.raises(ValueError):
        ValidatingBackend(inner, max_owner_length=-1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_key_length_property(backend):
    assert backend.max_key_length == 128


def test_max_owner_length_property(backend):
    assert backend.max_owner_length == 256


def test_acquire_delegates(inner, backend):
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 60)


def test_acquire_rejects_empty_key(backend):
    with pytest.raises(ValueError, match="lock key"):
        backend.acquire("", "worker-1", 60)


def test_acquire_rejects_empty_owner(backend):
    with pytest.raises(ValueError, match="owner"):
        backend.acquire("job", "", 60)


def test_acquire_rejects_key_too_long(inner):
    b = ValidatingBackend(inner, max_key_length=5)
    with pytest.raises(ValueError, match="max length"):
        b.acquire("toolongkey", "owner", 60)


def test_acquire_rejects_owner_too_long(inner):
    b = ValidatingBackend(inner, max_owner_length=4)
    with pytest.raises(ValueError, match="max length"):
        b.acquire("key", "toolongowner", 60)


def test_acquire_rejects_invalid_ttl(backend):
    with pytest.raises(ValueError, match="ttl"):
        backend.acquire("job", "worker", 0)


def test_release_delegates(inner, backend):
    result = backend.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_release_rejects_empty_key(backend):
    with pytest.raises(ValueError):
        backend.release("", "worker")


def test_is_locked_delegates(inner, backend):
    result = backend.is_locked("job")
    assert result is False
    inner.is_locked.assert_called_once_with("job")


def test_is_locked_rejects_empty_key(backend):
    with pytest.raises(ValueError):
        backend.is_locked("")


def test_refresh_delegates(inner, backend):
    result = backend.refresh("job", "worker", 30)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker", 30)
