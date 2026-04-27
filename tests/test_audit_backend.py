"""Tests for AuditBackend."""
from unittest.mock import MagicMock

import pytest

from schedlock.backends.audit_backend import AuditBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture()
def inner():
    m = MagicMock(spec=BaseBackend)
    m.acquire.return_value = True
    m.release.return_value = True
    m.is_locked.return_value = False
    m.refresh.return_value = True
    return m


@pytest.fixture()
def events():
    return []


@pytest.fixture()
def backend(inner, events):
    return AuditBackend(inner, sink=events.append)


def test_requires_inner_backend(events):
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        AuditBackend(object(), sink=events.append)


def test_requires_callable_sink(inner):
    with pytest.raises(TypeError, match="sink must be callable"):
        AuditBackend(inner, sink="not_callable")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_success_emits_event(backend, inner, events):
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    assert len(events) == 1
    ev = events[0]
    assert ev["event"] == "acquire"
    assert ev["key"] == "job"
    assert ev["owner"] == "worker-1"
    assert ev["ttl"] == 30
    assert ev["success"] is True


def test_acquire_failure_emits_event(backend, inner, events):
    inner.acquire.return_value = False
    result = backend.acquire("job", "worker-2", 60)
    assert result is False
    assert events[0]["success"] is False


def test_release_success_emits_event(backend, inner, events):
    result = backend.release("job", "worker-1")
    assert result is True
    ev = events[0]
    assert ev["event"] == "release"
    assert ev["ttl"] is None
    assert ev["success"] is True


def test_release_failure_emits_event(backend, inner, events):
    inner.release.return_value = False
    result = backend.release("job", "wrong-owner")
    assert result is False
    assert events[0]["success"] is False


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True


def test_refresh_delegates(backend, inner):
    assert backend.refresh("job", "worker-1", 30) is True
    inner.refresh.assert_called_once_with("job", "worker-1", 30)


def test_multiple_events_accumulate(backend, events):
    backend.acquire("a", "w1", 10)
    backend.acquire("b", "w2", 20)
    backend.release("a", "w1")
    assert len(events) == 3
    assert [e["event"] for e in events] == ["acquire", "acquire", "release"]
