"""Tests for LockQuota and QuotaBackend."""

import time
import pytest
from unittest.mock import MagicMock
from schedlock.quota import LockQuota
from schedlock.backends.quota_backend import QuotaBackend


@pytest.fixture
def quota():
    return LockQuota(max_acquisitions=3, window_seconds=60)


def test_quota_invalid_max():
    with pytest.raises(ValueError):
        LockQuota(max_acquisitions=0, window_seconds=60)


def test_quota_invalid_window():
    with pytest.raises(ValueError):
        LockQuota(max_acquisitions=1, window_seconds=0)


def test_allowed_initially(quota):
    assert quota.allowed("job") is True


def test_record_increments_count(quota):
    quota.record("job", "owner-1")
    assert quota.count("job") == 1


def test_quota_blocks_after_max(quota):
    for i in range(3):
        quota.record("job", f"owner-{i}")
    assert quota.allowed("job") is False


def test_quota_resets(quota):
    quota.record("job", "owner-1")
    quota.reset("job")
    assert quota.count("job") == 0
    assert quota.allowed("job") is True


def test_quota_prunes_expired_entries():
    q = LockQuota(max_acquisitions=2, window_seconds=0.1)
    q.record("job", "owner-1")
    q.record("job", "owner-2")
    assert q.allowed("job") is False
    time.sleep(0.15)
    assert q.allowed("job") is True


def test_quota_isolated_per_lock(quota):
    for i in range(3):
        quota.record("job-a", f"owner-{i}")
    assert quota.allowed("job-a") is False
    assert quota.allowed("job-b") is True


# QuotaBackend tests

@pytest.fixture
def mock_inner():
    b = MagicMock()
    b.acquire.return_value = True
    b.release.return_value = True
    b.is_locked.return_value = False
    b.refresh.return_value = True
    return b


@pytest.fixture
def backend(mock_inner):
    q = LockQuota(max_acquisitions=2, window_seconds=60)
    return QuotaBackend(inner=mock_inner, quota=q)


def test_quota_backend_acquire_success(backend, mock_inner):
    assert backend.acquire("job", "owner", 30) is True
    mock_inner.acquire.assert_called_once_with("job", "owner", 30)


def test_quota_backend_blocks_when_quota_exceeded(mock_inner):
    q = LockQuota(max_acquisitions=1, window_seconds=60)
    b = QuotaBackend(inner=mock_inner, quota=q)
    assert b.acquire("job", "owner-1", 30) is True
    assert b.acquire("job", "owner-2", 30) is False
    assert mock_inner.acquire.call_count == 1


def test_quota_backend_does_not_record_on_inner_failure(mock_inner):
    mock_inner.acquire.return_value = False
    q = LockQuota(max_acquisitions=2, window_seconds=60)
    b = QuotaBackend(inner=mock_inner, quota=q)
    assert b.acquire("job", "owner", 30) is False
    assert q.count("job") == 0


def test_quota_backend_delegates_release(backend, mock_inner):
    backend.release("job", "owner")
    mock_inner.release.assert_called_once_with("job", "owner")


def test_quota_backend_delegates_is_locked(backend, mock_inner):
    backend.is_locked("job")
    mock_inner.is_locked.assert_called_once_with("job")


def test_quota_backend_delegates_refresh(backend, mock_inner):
    backend.refresh("job", "owner", 30)
    mock_inner.refresh.assert_called_once_with("job", "owner", 30)
