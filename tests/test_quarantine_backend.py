import time
import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.quarantine_backend import QuarantineBackend


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
    return QuarantineBackend(inner, quarantine_seconds=5.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        QuarantineBackend("not-a-backend")


def test_requires_positive_quarantine_seconds(inner):
    with pytest.raises(ValueError):
        QuarantineBackend(inner, quarantine_seconds=0)
    with pytest.raises(ValueError):
        QuarantineBackend(inner, quarantine_seconds=-1)


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_quarantine_seconds_property(backend):
    assert backend.quarantine_seconds == 5.0


def test_acquire_delegates_when_not_quarantined(backend, inner):
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 30)


def test_acquire_blocked_when_quarantined(backend, inner):
    backend.quarantine("worker-bad")
    result = backend.acquire("job", "worker-bad", 30)
    assert result is False
    inner.acquire.assert_not_called()


def test_acquire_allowed_after_quarantine_expires(backend, inner):
    backend._quarantine_seconds = 0.05
    backend.quarantine("worker-temp")
    assert backend.acquire("job", "worker-temp", 30) is False
    time.sleep(0.1)
    assert backend.acquire("job", "worker-temp", 30) is True


def test_is_quarantined_true_while_active(backend):
    backend.quarantine("worker-x")
    assert backend.is_quarantined("worker-x") is True


def test_is_quarantined_false_for_unknown_owner(backend):
    assert backend.is_quarantined("unknown") is False


def test_is_quarantined_false_after_expiry(backend):
    backend._quarantine_seconds = 0.05
    backend.quarantine("worker-y")
    time.sleep(0.1)
    assert backend.is_quarantined("worker-y") is False


def test_release_delegates_to_inner(backend, inner):
    result = backend.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates_to_inner(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates_to_inner(backend, inner):
    result = backend.refresh("job", "worker-1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_quarantine_non_quarantined_owner_does_not_affect_others(backend, inner):
    backend.quarantine("bad-worker")
    result = backend.acquire("job", "good-worker", 30)
    assert result is True
    inner.acquire.assert_called_once_with("job", "good-worker", 30)
