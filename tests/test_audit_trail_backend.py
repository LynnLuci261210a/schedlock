"""Tests for AuditTrailBackend."""

from unittest.mock import MagicMock

import pytest

from schedlock.backends.audit_trail_backend import AuditTrailBackend, TrailEntry
from schedlock.backends.base import BaseBackend
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
    return AuditTrailBackend(inner)


# ---------------------------------------------------------------------------
# Construction validation
# ---------------------------------------------------------------------------

def test_requires_inner_backend():
    with pytest.raises(TypeError):
        AuditTrailBackend("not-a-backend")


def test_requires_positive_max_entries(inner):
    with pytest.raises(ValueError):
        AuditTrailBackend(inner, max_entries=0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_entries_property(inner):
    b = AuditTrailBackend(inner, max_entries=50)
    assert b.max_entries == 50


# ---------------------------------------------------------------------------
# Acquire recording
# ---------------------------------------------------------------------------

def test_acquire_success_records_ok(backend):
    backend.acquire("job", "worker-1", 30)
    assert len(backend.trail) == 1
    entry = backend.trail[0]
    assert entry.action == "acquire_ok"
    assert entry.key == "job"
    assert entry.owner == "worker-1"
    assert entry.ttl == 30


def test_acquire_failure_records_fail(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job", "worker-1", 30)
    assert backend.trail[0].action == "acquire_fail"


# ---------------------------------------------------------------------------
# Release recording
# ---------------------------------------------------------------------------

def test_release_success_records_ok(backend):
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    assert backend.trail[1].action == "release_ok"
    assert backend.trail[1].ttl is None


def test_release_failure_records_fail(inner, backend):
    inner.release.return_value = False
    backend.release("job", "worker-1")
    assert backend.trail[0].action == "release_fail"


# ---------------------------------------------------------------------------
# trail_for filtering
# ---------------------------------------------------------------------------

def test_trail_for_filters_by_key(backend):
    backend.acquire("job-a", "w1", 10)
    backend.acquire("job-b", "w2", 10)
    backend.release("job-a", "w1")
    assert all(e.key == "job-a" for e in backend.trail_for("job-a"))
    assert len(backend.trail_for("job-a")) == 2
    assert len(backend.trail_for("job-b")) == 1


# ---------------------------------------------------------------------------
# Max-entries eviction
# ---------------------------------------------------------------------------

def test_max_entries_evicts_oldest(inner):
    b = AuditTrailBackend(inner, max_entries=3)
    for i in range(5):
        b.acquire(f"job-{i}", "w", 10)
    assert len(b.trail) == 3
    assert b.trail[0].key == "job-2"


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(inner, backend):
    assert backend.refresh("job", "w", 60) is True
    inner.refresh.assert_called_once_with("job", "w", 60)


# ---------------------------------------------------------------------------
# Integration with MemoryBackend
# ---------------------------------------------------------------------------

def test_integration_with_memory_backend():
    mem = MemoryBackend()
    b = AuditTrailBackend(mem)
    assert b.acquire("task", "owner-1", 60) is True
    assert b.acquire("task", "owner-2", 60) is False  # already locked
    assert b.release("task", "owner-1") is True
    actions = [e.action for e in b.trail]
    assert actions == ["acquire_ok", "acquire_fail", "release_ok"]
