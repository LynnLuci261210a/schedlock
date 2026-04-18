"""Tests for SnapshotBackend."""

import pytest
from unittest.mock import MagicMock

from schedlock.backends.snapshot_backend import SnapshotBackend, Snapshot
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    return m


@pytest.fixture
def backend(inner):
    return SnapshotBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        SnapshotBackend(inner="not-a-backend")


def test_requires_positive_max_snapshots(inner):
    with pytest.raises(ValueError):
        SnapshotBackend(inner, max_snapshots=0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_success_records_snapshot(inner, backend):
    result = backend.acquire("job1", "worker-1", 60)
    assert result is True
    snaps = backend.snapshots()
    assert len(snaps) == 1
    assert snaps[0].action == "acquired"
    assert snaps[0].job_name == "job1"
    assert snaps[0].owner == "worker-1"
    assert snaps[0].result is True
    assert snaps[0].ttl == 60


def test_acquire_blocked_records_snapshot(inner, backend):
    inner.acquire.return_value = False
    backend.acquire("job1", "worker-2", 30)
    snaps = backend.snapshots()
    assert snaps[0].action == "blocked"
    assert snaps[0].result is False


def test_release_records_snapshot(inner, backend):
    backend.release("job1", "worker-1")
    snaps = backend.snapshots()
    assert snaps[0].action == "released"
    assert snaps[0].ttl is None


def test_is_locked_records_snapshot(inner, backend):
    backend.is_locked("job1")
    snaps = backend.snapshots()
    assert snaps[0].action == "checked"


def test_snapshots_filtered_by_job(inner, backend):
    backend.acquire("job1", "w1", 60)
    backend.acquire("job2", "w2", 60)
    assert len(backend.snapshots("job1")) == 1
    assert len(backend.snapshots("job2")) == 1
    assert len(backend.snapshots()) == 2


def test_max_snapshots_evicts_oldest(inner):
    backend = SnapshotBackend(inner, max_snapshots=3)
    for i in range(5):
        backend.acquire(f"job{i}", "w", 60)
    snaps = backend.snapshots()
    assert len(snaps) == 3
    assert snaps[0].job_name == "job2"


def test_clear_snapshots(inner, backend):
    backend.acquire("job1", "w", 60)
    backend.clear_snapshots()
    assert backend.snapshots() == []


def test_snapshot_str_format(inner, backend):
    backend.acquire("myjob", "owner-x", 10)
    s = str(backend.snapshots()[0])
    assert "acquired" in s
    assert "myjob" in s
    assert "owner-x" in s


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    result = backend.refresh("job1", "w", 60)
    assert result is True
    inner.refresh.assert_called_once_with("job1", "w", 60)
