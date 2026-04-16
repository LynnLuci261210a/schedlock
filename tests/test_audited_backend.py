"""Tests for AuditedBackend wrapper."""

import pytest
from unittest.mock import MagicMock
from schedlock.backends.audited_backend import AuditedBackend
from schedlock.audit import LockAuditLog


@pytest.fixture
def inner():
    m = MagicMock()
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    m.get_owner.return_value = "owner1"
    return m


@pytest.fixture
def audit_log():
    return LockAuditLog(log_to_logger=False)


@pytest.fixture
def backend(inner, audit_log):
    return AuditedBackend(inner, audit_log)


def test_acquire_success_records_acquired(backend, audit_log, inner):
    result = backend.acquire("job:x", "owner1", 60)
    assert result is True
    entries = audit_log.entries(event="acquired")
    assert len(entries) == 1
    assert entries[0].lock_key == "job:x"
    assert entries[0].ttl == 60


def test_acquire_failure_records_failed(backend, audit_log, inner):
    inner.acquire.return_value = False
    result = backend.acquire("job:x", "owner1", 60)
    assert result is False
    entries = audit_log.entries(event="failed")
    assert len(entries) == 1


def test_release_success_records_released(backend, audit_log):
    backend.release("job:x", "owner1")
    entries = audit_log.entries(event="released")
    assert len(entries) == 1
    assert entries[0].owner == "owner1"


def test_release_failure_does_not_record(backend, audit_log, inner):
    inner.release.return_value = False
    backend.release("job:x", "owner1")
    assert len(audit_log.entries(event="released")) == 0


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job:x") is True
    inner.is_locked.assert_called_once_with("job:x")


def test_refresh_delegates(backend, inner):
    assert backend.refresh("job:x", "owner1", 30) is True
    inner.refresh.assert_called_once_with("job:x", "owner1", 30)


def test_get_owner_delegates(backend, inner):
    assert backend.get_owner("job:x") == "owner1"
    inner.get_owner.assert_called_once_with("job:x")


def test_default_audit_log_created_if_none(inner):
    b = AuditedBackend(inner)
    assert isinstance(b.audit_log, LockAuditLog)


def test_multiple_operations_all_logged(backend, audit_log):
    backend.acquire("job:a", "o1", 10)
    backend.acquire("job:b", "o2", 20)
    backend.release("job:a", "o1")
    assert len(audit_log) == 3
