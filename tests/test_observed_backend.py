import pytest
from unittest.mock import MagicMock, patch

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.observer_backend import ObservedBackend
from schedlock.observer import LockObserver, LockEvent
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    return MemoryBackend()


@pytest.fixture
def observer():
    return LockObserver()


@pytest.fixture
def backend(inner, observer):
    return ObservedBackend(inner=inner, observer=observer)


def test_requires_inner_backend(observer):
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        ObservedBackend(inner="not-a-backend", observer=observer)


def test_requires_observer(inner):
    with pytest.raises(TypeError, match="observer must be a LockObserver instance"):
        ObservedBackend(inner=inner, observer="not-an-observer")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_observer_property(backend, observer):
    assert backend.observer is observer


def test_acquire_success_emits_acquired_event(backend, observer):
    events = []
    observer.subscribe(events.append)
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    assert len(events) == 1
    assert events[0].event == "acquired"
    assert events[0].key == "job"
    assert events[0].owner == "worker-1"


def test_acquire_blocked_emits_blocked_event(backend, observer):
    events = []
    observer.subscribe(events.append)
    backend.acquire("job", "worker-1", 60)
    result = backend.acquire("job", "worker-2", 60)
    assert result is False
    blocked_events = [e for e in events if e.event == "blocked"]
    assert len(blocked_events) == 1
    assert blocked_events[0].owner == "worker-2"


def test_release_success_emits_released_event(backend, observer):
    events = []
    observer.subscribe(events.append)
    backend.acquire("job", "worker-1", 60)
    events.clear()
    result = backend.release("job", "worker-1")
    assert result is True
    assert len(events) == 1
    assert events[0].event == "released"
    assert events[0].key == "job"
    assert events[0].owner == "worker-1"


def test_release_failure_does_not_emit_event(backend, observer):
    events = []
    observer.subscribe(events.append)
    result = backend.release("nonexistent", "worker-1")
    assert result is False
    assert len(events) == 0


def test_is_locked_delegates(backend):
    assert backend.is_locked("job") is False
    backend.acquire("job", "worker-1", 60)
    assert backend.is_locked("job") is True


def test_refresh_delegates(backend):
    backend.acquire("job", "worker-1", 60)
    result = backend.refresh("job", "worker-1", 120)
    assert result is True


def test_multiple_handlers_all_notified(backend, observer):
    received_a = []
    received_b = []
    observer.subscribe(received_a.append)
    observer.subscribe(received_b.append)
    backend.acquire("job", "worker-1", 60)
    assert len(received_a) == 1
    assert len(received_b) == 1
