"""Unit tests for MaintenanceBackend."""
import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.maintenance_backend import MaintenanceBackend


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
    return MaintenanceBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        MaintenanceBackend("not-a-backend")


def test_requires_non_empty_reason(inner):
    with pytest.raises(ValueError):
        MaintenanceBackend(inner, reason="")
    with pytest.raises(ValueError):
        MaintenanceBackend(inner, reason="   ")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_reason_property(inner):
    b = MaintenanceBackend(inner, reason="deploy")
    assert b.reason == "deploy"


def test_default_reason(backend):
    assert backend.reason == "maintenance"


def test_is_in_maintenance_initially_false(backend):
    assert backend.is_in_maintenance is False


def test_acquire_succeeds_when_not_in_maintenance(inner, backend):
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 60)


def test_acquire_blocked_during_maintenance(inner, backend):
    backend.enter_maintenance()
    result = backend.acquire("job", "worker-1", 60)
    assert result is False
    inner.acquire.assert_not_called()


def test_exit_maintenance_restores_acquire(inner, backend):
    backend.enter_maintenance()
    backend.exit_maintenance()
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    inner.acquire.assert_called_once()


def test_release_delegates_during_maintenance(inner, backend):
    backend.enter_maintenance()
    result = backend.release("job", "worker-1")
    assert result is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates_during_maintenance(inner, backend):
    inner.is_locked.return_value = True
    backend.enter_maintenance()
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_blocked_during_maintenance(inner, backend):
    backend.enter_maintenance()
    result = backend.refresh("job", "worker-1", 60)
    assert result is False
    inner.refresh.assert_not_called()


def test_refresh_delegates_when_not_in_maintenance(inner, backend):
    result = backend.refresh("job", "worker-1", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job", "worker-1", 60)


def test_enter_maintenance_is_idempotent(inner, backend):
    backend.enter_maintenance()
    backend.enter_maintenance()
    assert backend.is_in_maintenance is True


def test_exit_maintenance_is_idempotent(backend):
    backend.exit_maintenance()
    assert backend.is_in_maintenance is False
