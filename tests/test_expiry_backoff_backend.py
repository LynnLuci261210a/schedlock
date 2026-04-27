"""Tests for ExpiryBackoffBackend."""

from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.expiry_backoff_backend import ExpiryBackoffBackend
from schedlock.backends.memory_backend import MemoryBackend


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
    return ExpiryBackoffBackend(inner, growth_factor=2.0, max_ttl=300.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        ExpiryBackoffBackend(object())  # type: ignore[arg-type]


def test_requires_growth_factor_above_one(inner):
    with pytest.raises(ValueError, match="growth_factor"):
        ExpiryBackoffBackend(inner, growth_factor=1.0)


def test_requires_positive_max_ttl(inner):
    with pytest.raises(ValueError, match="max_ttl"):
        ExpiryBackoffBackend(inner, max_ttl=0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_growth_factor_property(inner, backend):
    assert backend.growth_factor == 2.0


def test_max_ttl_property(inner, backend):
    assert backend.max_ttl == 300.0


def test_acquire_success_no_backoff(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "w1", ttl=60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "w1", 60.0)
    assert backend.failure_count("job") == 0
    assert backend.current_multiplier("job") == 1.0


def test_acquire_failure_increments_failure_count(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "w1", ttl=60)
    assert backend.failure_count("job") == 1


def test_acquire_failure_grows_multiplier(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "w1", ttl=60)
    assert backend.current_multiplier("job") == 2.0
    backend.acquire("job", "w1", ttl=60)
    assert backend.current_multiplier("job") == 4.0


def test_effective_ttl_grows_after_failures(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "w1", ttl=60)
    # Second attempt: multiplier is 2.0 → TTL should be 120
    backend.acquire("job", "w1", ttl=60)
    calls = inner.acquire.call_args_list
    assert calls[1][0][2] == 120.0


def test_effective_ttl_capped_at_max_ttl(inner, backend):
    inner.acquire.return_value = False
    for _ in range(10):
        backend.acquire("job", "w1", ttl=60)
    assert backend.current_multiplier("job") == 300.0  # capped at max_ttl


def test_successful_acquire_resets_backoff(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "w1", ttl=60)
    backend.acquire("job", "w1", ttl=60)
    assert backend.failure_count("job") == 2

    inner.acquire.return_value = True
    backend.acquire("job", "w1", ttl=60)
    assert backend.failure_count("job") == 0
    assert backend.current_multiplier("job") == 1.0


def test_release_resets_backoff(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "w1", ttl=60)
    inner.release.return_value = True
    backend.release("job", "w1")
    assert backend.failure_count("job") == 0
    assert backend.current_multiplier("job") == 1.0


def test_release_failed_does_not_reset(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "w1", ttl=60)
    inner.release.return_value = False
    backend.release("job", "w1")
    assert backend.failure_count("job") == 1  # still there


def test_none_ttl_passed_through(inner, backend):
    inner.acquire.return_value = True
    backend.acquire("job", "w1", ttl=None)
    inner.acquire.assert_called_once_with("job", "w1", None)


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    assert backend.refresh("job", "w1", 30) is True
    inner.refresh.assert_called_once_with("job", "w1", 30)


def test_independent_keys_tracked_separately(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("key_a", "w1", ttl=60)
    backend.acquire("key_a", "w1", ttl=60)
    assert backend.failure_count("key_a") == 2
    assert backend.failure_count("key_b") == 0
    assert backend.current_multiplier("key_b") == 1.0


def test_integration_with_memory_backend():
    mem = MemoryBackend()
    b = ExpiryBackoffBackend(mem, growth_factor=3.0, max_ttl=500.0)

    assert b.acquire("job", "w1", ttl=10) is True
    assert b.failure_count("job") == 0

    # Second owner is blocked
    assert b.acquire("job", "w2", ttl=10) is False
    assert b.failure_count("job") == 1
    assert b.current_multiplier("job") == 3.0

    # Release and reacquire resets
    assert b.release("job", "w1") is True
    assert b.failure_count("job") == 0
