import time
import pytest
from unittest.mock import MagicMock
from schedlock.backends.debounce_backend import DebounceBackend
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
    return DebounceBackend(inner, cooldown=1.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        DebounceBackend("not-a-backend")


def test_requires_positive_cooldown(inner):
    with pytest.raises(ValueError):
        DebounceBackend(inner, cooldown=0)
    with pytest.raises(ValueError):
        DebounceBackend(inner, cooldown=-1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_cooldown_property(backend):
    assert backend.cooldown == 1.0


def test_acquire_succeeds_first_time(inner, backend):
    assert backend.acquire("job", "owner1", 30) is True
    inner.acquire.assert_called_once_with("job", "owner1", 30)


def test_acquire_debounced_within_cooldown(inner, backend):
    backend.acquire("job", "owner1", 30)
    result = backend.acquire("job", "owner1", 30)
    assert result is False
    assert inner.acquire.call_count == 1


def test_acquire_allowed_after_cooldown(inner, backend):
    backend.acquire("job", "owner1", 30)
    # Manually expire the cooldown
    backend._last_acquired["job"] = time.time() - 2.0
    result = backend.acquire("job", "owner1", 30)
    assert result is True
    assert inner.acquire.call_count == 2


def test_release_clears_debounce_state(inner, backend):
    backend.acquire("job", "owner1", 30)
    backend.release("job", "owner1")
    assert "job" not in backend._last_acquired


def test_release_delegates_to_inner(inner, backend):
    backend.release("job", "owner1")
    inner.release.assert_called_once_with("job", "owner1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    assert backend.refresh("job", "owner1", 60) is True
    inner.refresh.assert_called_once_with("job", "owner1", 60)


def test_different_keys_debounced_independently(inner, backend):
    backend.acquire("job_a", "owner1", 30)
    result = backend.acquire("job_b", "owner1", 30)
    assert result is True
    assert inner.acquire.call_count == 2
