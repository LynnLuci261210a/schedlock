"""Tests for TimedBackend."""

import pytest
from unittest.mock import MagicMock

from schedlock.backends.timed_backend import TimedBackend, TimingRecord
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
    return TimedBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        TimedBackend(inner="not-a-backend")


def test_requires_positive_max_records(inner):
    with pytest.raises(ValueError):
        TimedBackend(inner, max_records=0)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_delegates_and_records(inner, backend):
    result = backend.acquire("myjob", "owner1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("myjob", "owner1", 60)
    assert len(backend.records) == 1
    rec = backend.records[0]
    assert rec.operation == "acquire"
    assert rec.job_name == "myjob"
    assert rec.success is True
    assert rec.duration_ms >= 0


def test_acquire_failure_recorded(inner, backend):
    inner.acquire.return_value = False
    result = backend.acquire("myjob", "owner1", 60)
    assert result is False
    assert backend.records[0].success is False


def test_release_delegates_and_records(inner, backend):
    result = backend.release("myjob", "owner1")
    assert result is True
    inner.release.assert_called_once_with("myjob", "owner1")
    rec = backend.records[0]
    assert rec.operation == "release"
    assert rec.success is True


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("myjob") is True
    inner.is_locked.assert_called_once_with("myjob")


def test_refresh_delegates(inner, backend):
    assert backend.refresh("myjob", "owner1", 30) is True
    inner.refresh.assert_called_once_with("myjob", "owner1", 30)


def test_max_records_evicts_oldest(inner):
    backend = TimedBackend(inner, max_records=3)
    for _ in range(5):
        backend.acquire("myjob", "owner", 60)
    assert len(backend.records) == 3


def test_average_duration_ms_all(inner, backend):
    backend.acquire("myjob", "owner", 60)
    backend.release("myjob", "owner")
    avg = backend.average_duration_ms()
    assert avg >= 0.0


def test_average_duration_ms_by_operation(inner, backend):
    backend.acquire("myjob", "owner", 60)
    backend.release("myjob", "owner")
    avg_acquire = backend.average_duration_ms(operation="acquire")
    avg_release = backend.average_duration_ms(operation="release")
    assert avg_acquire >= 0.0
    assert avg_release >= 0.0


def test_average_duration_ms_empty(backend):
    assert backend.average_duration_ms() == 0.0


def test_timing_record_str():
    rec = TimingRecord(job_name="j", operation="acquire", duration_ms=12.5, success=True)
    assert "acquire" in str(rec)
    assert "j" in str(rec)
