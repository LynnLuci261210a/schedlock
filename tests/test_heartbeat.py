"""Tests for schedlock.heartbeat.LockHeartbeat."""

import time
import threading
import pytest
from unittest.mock import MagicMock, call
from schedlock.heartbeat import LockHeartbeat


@pytest.fixture
def mock_backend():
    backend = MagicMock()
    backend.refresh.return_value = True
    return backend


def test_heartbeat_starts_and_stops(mock_backend):
    hb = LockHeartbeat(mock_backend, "my_lock", "owner-1", ttl=30, interval=1)
    hb.start()
    assert hb._thread is not None
    assert hb._thread.is_alive()
    hb.stop()
    assert not hb._thread.is_alive()


def test_heartbeat_calls_refresh_periodically(mock_backend):
    hb = LockHeartbeat(mock_backend, "job_lock", "owner-42", ttl=10, interval=1)
    hb.start()
    time.sleep(2.5)
    hb.stop()
    assert mock_backend.refresh.call_count >= 2
    mock_backend.refresh.assert_called_with("job_lock", "owner-42", 10)


def test_heartbeat_stops_refreshing_after_stop(mock_backend):
    hb = LockHeartbeat(mock_backend, "job_lock", "owner-1", ttl=10, interval=1)
    hb.start()
    time.sleep(1.3)
    hb.stop()
    count_after_stop = mock_backend.refresh.call_count
    time.sleep(1.5)
    assert mock_backend.refresh.call_count == count_after_stop


def test_heartbeat_logs_warning_when_refresh_fails(mock_backend, caplog):
    import logging
    mock_backend.refresh.return_value = False
    hb = LockHeartbeat(mock_backend, "expiring_lock", "owner-x", ttl=5, interval=1)
    with caplog.at_level(logging.WARNING, logger="schedlock.heartbeat"):
        hb.start()
        time.sleep(1.5)
        hb.stop()
    assert any("failed to refresh" in r.message for r in caplog.records)


def test_heartbeat_context_manager(mock_backend):
    with LockHeartbeat(mock_backend, "ctx_lock", "owner-ctx", ttl=10, interval=1) as hb:
        assert hb._thread.is_alive()
        time.sleep(1.3)
    assert not hb._thread.is_alive()
    assert mock_backend.refresh.call_count >= 1


def test_heartbeat_default_interval():
    backend = MagicMock()
    hb = LockHeartbeat(backend, "lock", "owner", ttl=60)
    assert hb.interval == 20


def test_heartbeat_default_interval_minimum():
    backend = MagicMock()
    hb = LockHeartbeat(backend, "lock", "owner", ttl=2)
    assert hb.interval == 1


def test_heartbeat_thread_is_daemon(mock_backend):
    hb = LockHeartbeat(mock_backend, "lock", "owner", ttl=30, interval=5)
    hb.start()
    assert hb._thread.daemon is True
    hb.stop()
