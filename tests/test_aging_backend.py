"""Tests for AgingBackend."""
import pytest
from unittest.mock import MagicMock

from schedlock.backends.aging_backend import AgingBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return AgingBackend(inner, growth_factor=2.0, max_ttl=500.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        AgingBackend("not-a-backend")


def test_requires_growth_factor_above_one(inner):
    with pytest.raises(ValueError):
        AgingBackend(inner, growth_factor=1.0)
    with pytest.raises(ValueError):
        AgingBackend(inner, growth_factor=0.5)


def test_requires_positive_max_ttl(inner):
    with pytest.raises(ValueError):
        AgingBackend(inner, max_ttl=0)
    with pytest.raises(ValueError):
        AgingBackend(inner, max_ttl=-10)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_growth_factor_property(inner, backend):
    assert backend.growth_factor == 2.0


def test_max_ttl_property(inner, backend):
    assert backend.max_ttl == 500.0


def test_first_acquire_uses_original_ttl(inner, backend):
    inner.acquire.return_value = True
    backend.acquire("job", "worker-1", 60.0)
    inner.acquire.assert_called_once_with("job", "worker-1", 60.0)


def test_second_acquire_grows_ttl(inner, backend):
    inner.acquire.return_value = True
    backend.acquire("job", "worker-1", 60.0)  # count becomes 1
    inner.acquire.reset_mock()
    backend.acquire("job", "worker-1", 60.0)  # should use 60 * 2^1 = 120
    args = inner.acquire.call_args[0]
    assert args[2] == pytest.approx(120.0)


def test_ttl_capped_at_max(inner):
    b = AgingBackend(inner, growth_factor=10.0, max_ttl=100.0)
    inner.acquire.return_value = True
    for _ in range(5):
        b.acquire("job", "w", 60.0)
    args = inner.acquire.call_args[0]
    assert args[2] <= 100.0


def test_release_resets_age(inner, backend):
    inner.acquire.return_value = True
    inner.release.return_value = True
    backend.acquire("job", "worker-1", 60.0)
    backend.acquire("job", "worker-1", 60.0)
    backend.release("job", "worker-1")
    inner.acquire.reset_mock()
    backend.acquire("job", "worker-1", 60.0)
    args = inner.acquire.call_args[0]
    assert args[2] == pytest.approx(60.0)


def test_failed_acquire_does_not_increment_age(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 60.0)
    inner.acquire.reset_mock()
    inner.acquire.return_value = True
    backend.acquire("job", "worker-1", 60.0)
    args = inner.acquire.call_args[0]
    assert args[2] == pytest.approx(60.0)


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    assert backend.refresh("job", "w", 30.0) is True
    inner.refresh.assert_called_once_with("job", "w", 30.0)
