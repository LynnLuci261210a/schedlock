import time
import pytest
from unittest.mock import MagicMock
from schedlock.backends.cooldown_backend import CooldownBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=["acquire", "release", "is_locked", "refresh"])


@pytest.fixture
def backend(inner):
    return CooldownBackend(inner, cooldown=1.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        CooldownBackend("not-a-backend", cooldown=1.0)


def test_requires_positive_cooldown():
    inner = MagicMock(spec=["acquire", "release", "is_locked", "refresh"])
    with pytest.raises(ValueError):
        CooldownBackend(inner, cooldown=0)
    with pytest.raises(ValueError):
        CooldownBackend(inner, cooldown=-5)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_cooldown_property(backend):
    assert backend.cooldown == 1.0


def test_acquire_delegates_to_inner(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "owner-1", 30)


def test_acquire_blocked_during_cooldown(inner, backend):
    inner.release.return_value = True
    backend.release("job", "owner-1")
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner-2", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_allowed_after_cooldown_expires(inner):
    b = CooldownBackend(inner, cooldown=0.05)
    inner.release.return_value = True
    b.release("job", "owner-1")
    time.sleep(0.1)
    inner.acquire.return_value = True
    result = b.acquire("job", "owner-2", 30)
    assert result is True


def test_release_records_timestamp_only_on_success(inner, backend):
    inner.release.return_value = False
    backend.release("job", "owner-1")
    # No cooldown recorded — acquire should still delegate
    inner.acquire.return_value = True
    result = backend.acquire("job", "owner-2", 30)
    assert result is True


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    assert backend.refresh("job", "owner-1", 60) is True
    inner.refresh.assert_called_once_with("job", "owner-1", 60)


def test_integration_with_memory_backend():
    mem = MemoryBackend()
    b = CooldownBackend(mem, cooldown=0.05)
    assert b.acquire("job", "worker-1", 30) is True
    assert b.release("job", "worker-1") is True
    # Immediately blocked by cooldown
    assert b.acquire("job", "worker-2", 30) is False
    time.sleep(0.1)
    # Now allowed
    assert b.acquire("job", "worker-2", 30) is True
