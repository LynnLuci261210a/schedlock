"""Tests for schedlock.audit module."""

import time
import pytest
from schedlock.audit import AuditEntry, LockAuditLog


@pytest.fixture
def log():
    return LockAuditLog(max_entries=10, log_to_logger=False)


def test_record_creates_entry(log):
    entry = log.record("acquired", "job:backup", "host1")
    assert isinstance(entry, AuditEntry)
    assert entry.event == "acquired"
    assert entry.lock_key == "job:backup"
    assert entry.owner == "host1"


def test_entry_timestamp_is_recent(log):
    before = time.time()
    entry = log.record("acquired", "job:x", "owner1")
    after = time.time()
    assert before <= entry.timestamp <= after


def test_record_with_ttl_and_note(log):
    entry = log.record("released", "job:x", "owner1", ttl=60, note="clean exit")
    assert entry.ttl == 60
    assert entry.note == "clean exit"


def test_entries_returns_all(log):
    log.record("acquired", "job:a", "o1")
    log.record("released", "job:a", "o1")
    assert len(log.entries()) == 2


def test_entries_filter_by_lock_key(log):
    log.record("acquired", "job:a", "o1")
    log.record("acquired", "job:b", "o2")
    result = log.entries(lock_key="job:a")
    assert len(result) == 1
    assert result[0].lock_key == "job:a"


def test_entries_filter_by_event(log):
    log.record("acquired", "job:a", "o1")
    log.record("failed", "job:a", "o2")
    result = log.entries(event="failed")
    assert len(result) == 1
    assert result[0].event == "failed"


def test_max_entries_evicts_oldest(log):
    for i in range(12):
        log.record("acquired", f"job:{i}", "owner")
    assert len(log) == 10
    assert log.entries()[0].lock_key == "job:2"


def test_clear_removes_all(log):
    log.record("acquired", "job:a", "o1")
    log.clear()
    assert len(log) == 0


def test_str_representation():
    entry = AuditEntry(event="acquired", lock_key="job:x", owner="host1", ttl=30)
    s = str(entry)
    assert "ACQUIRED" in s
    assert "job:x" in s
    assert "host1" in s
    assert "ttl=30" in s


def test_len(log):
    log.record("acquired", "job:a", "o")
    log.record("released", "job:a", "o")
    assert len(log) == 2
