"""Unit tests for StickyTTLBackend."""

from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.sticky_ttl_backend import StickyTTLBackend


@pytest.fixture()
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture()
def backend(inner):
    return StickyTTLBackend(inner, growth_factor=2.0, max_ttl=120.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        StickyTTLBackend("not-a-backend")


def test_requires_growth_factor_above_one(inner):
    with pytest.raises(ValueError):
        StickyTTLBackend(inner, growth_factor=1.0)
    with pytest.raises(ValueError):
        StickyTTLBackend(inner, growth_factor=0.5)


def test_requires_positive_max_ttl(inner):
    with pytest.raises(ValueError):
        StickyTTLBackend(inner, max_ttl=0)
    with pytest.raises(ValueError):
        StickyTTLBackend(inner, max_ttl=-10)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_growth_factor_property(backend):
    assert backend.growth_factor == 2.0


def test_max_ttl_property(backend):
    assert backend.max_ttl == 120.0


def test_acquire_uses_original_ttl_first_time(inner, backend):
    backend.acquire("job", "worker-1", 30)
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_grows_ttl_on_reacquire(inner, backend):
    backend.acquire("job", "worker-1", 30)
    backend.acquire("job", "worker-1", 30)
    calls = inner.acquire.call_args_list
    assert calls[0][0] == ("job", "worker-1", 30)
    assert calls[1][0] == ("job", "worker-1", 60)  # 30 * 2.0


def test_ttl_capped_at_max_ttl(inner, backend):
    backend.acquire("job", "worker-1", 100)
    backend.acquire("job", "worker-1", 100)  # 100 * 2 = 200 -> capped at 120
    calls = inner.acquire.call_args_list
    assert calls[1][0] == ("job", "worker-1", 120)


def test_effective_ttl_for_returns_none_before_acquire(backend):
    assert backend.effective_ttl_for("job", "worker-1") is None


def test_effective_ttl_for_returns_value_after_acquire(backend):
    backend.acquire("job", "worker-1", 30)
    assert backend.effective_ttl_for("job", "worker-1") == 30.0


def test_release_clears_ttl_map(inner, backend):
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    assert backend.effective_ttl_for("job", "worker-1") is None


def test_release_does_not_clear_on_failure(inner, backend):
    inner.release.return_value = False
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    assert backend.effective_ttl_for("job", "worker-1") == 30.0


def test_failed_acquire_does_not_update_ttl_map(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 30)
    assert backend.effective_ttl_for("job", "worker-1") is None


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    result = backend.refresh("job", "worker-1", 30)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker-1", 30)


def test_different_owners_tracked_independently(inner, backend):
    backend.acquire("job", "worker-1", 10)
    backend.acquire("job", "worker-2", 10)
    backend.acquire("job", "worker-1", 10)
    calls = {c[0]: c for c in inner.acquire.call_args_list}
    # worker-1 second call should have grown TTL; worker-2 first call stays at 10
    assert inner.acquire.call_args_list[2][0] == ("job", "worker-1", 20)
    assert inner.acquire.call_args_list[1][0] == ("job", "worker-2", 10)
