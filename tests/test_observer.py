"""Tests for schedlock.observer module."""

import pytest
from schedlock.observer import LockEvent, LockObserver, default_observer


@pytest.fixture
def observer():
    return LockObserver()


def test_lock_event_fields():
    event = LockEvent(event_type="acquired", lock_name="my_job", owner="host:123")
    assert event.event_type == "acquired"
    assert event.lock_name == "my_job"
    assert event.owner == "host:123"
    assert event.backend == "unknown"
    assert event.extra == {}


def test_subscribe_and_notify(observer):
    received = []
    observer.subscribe(received.append)
    observer.emit("acquired", "job", "owner1", backend="memory")
    assert len(received) == 1
    assert received[0].event_type == "acquired"
    assert received[0].backend == "memory"


def test_multiple_handlers(observer):
    calls_a, calls_b = [], []
    observer.subscribe(calls_a.append)
    observer.subscribe(calls_b.append)
    observer.emit("released", "job", "owner1")
    assert len(calls_a) == 1
    assert len(calls_b) == 1


def test_unsubscribe(observer):
    received = []
    observer.subscribe(received.append)
    observer.unsubscribe(received.append)
    observer.emit("failed", "job", "owner1")
    assert received == []


def test_unsubscribe_nonexistent_is_safe(observer):
    def handler(e): pass
    observer.unsubscribe(handler)  # should not raise


def test_emit_extra_kwargs(observer):
    received = []
    observer.subscribe(received.append)
    observer.emit("acquired", "job", "owner", ttl=60, retries=2)
    assert received[0].extra == {"ttl": 60, "retries": 2}


def test_notify_with_event_object(observer):
    received = []
    observer.subscribe(received.append)
    event = LockEvent("expired", "job", "owner", backend="redis")
    observer.notify(event)
    assert received[0] is event


def test_default_observer_is_lock_observer():
    assert isinstance(default_observer, LockObserver)


def test_default_observer_can_subscribe():
    received = []
    default_observer.subscribe(received.append)
    default_observer.emit("acquired", "test_job", "owner")
    assert len(received) >= 1
    default_observer.unsubscribe(received.append)
