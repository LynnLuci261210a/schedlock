"""Tests for TTLCapBackend."""

import pytest
from unittest.mock import MagicMock
from schedlock.backends.ttl_backend import TTLCapBackend
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
    return TTLCapBackend(inner, max_ttl=300)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        TTLCapBackend("not-a-backend")


def test_max_ttl_must_be_positive():
    inner = MagicMock(spec=BaseBackend)
    with pytest.raises(ValueError):
        TTLCapBackend(inner, max_ttl=0)
    with pytest.raises(ValueError):
        TTLCapBackend(inner, max_ttl=-10)


def test_acquire_caps_ttl(inner, backend):
    backend.acquire("job", "owner1", ttl=9999)
    inner.acquire.assert_called_once_with("job", "owner1", 300)


def test_acquire_passes_lower_ttl_unchanged(inner, backend):
    backend.acquire("job", "owner1", ttl=60)
    inner.acquire.assert_called_once_with("job", "owner1", 60)


def test_acquire_returns_inner_result(inner, backend):
    inner.acquire.return_value = False
    assert backend.acquire("job", "owner1", ttl=100) is False


def test_release_delegates(inner, backend):
    backend.release("job", "owner1")
    inner.release.assert_called_once_with("job", "owner1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_caps_ttl(inner, backend):
    backend.refresh("job", "owner1", ttl=600)
    inner.refresh.assert_called_once_with("job", "owner1", 300)


def test_refresh_passes_lower_ttl_unchanged(inner, backend):
    backend.refresh("job", "owner1", ttl=120)
    inner.refresh.assert_called_once_with("job", "owner1", 120)


def test_max_ttl_property(backend):
    assert backend.max_ttl == 300


def test_is_base_subclass():
    assert issubclass(TTLCapBackend, BaseBackend)
