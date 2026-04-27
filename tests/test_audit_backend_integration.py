"""Integration tests for AuditBackend over MemoryBackend."""
import pytest

from schedlock.backends.audit_backend import AuditBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture()
def events():
    return []


@pytest.fixture()
def memory():
    return MemoryBackend()


@pytest.fixture()
def backend(memory, events):
    return AuditBackend(memory, sink=events.append)


def test_full_lifecycle_recorded(backend, events):
    assert backend.acquire("cron:daily", "host-1", 60) is True
    assert backend.release("cron:daily", "host-1") is True
    assert len(events) == 2
    assert events[0]["event"] == "acquire"
    assert events[1]["event"] == "release"


def test_second_worker_blocked_is_recorded(backend, events):
    backend.acquire("cron:daily", "host-1", 60)
    result = backend.acquire("cron:daily", "host-2", 60)
    assert result is False
    assert events[1]["success"] is False
    assert events[1]["owner"] == "host-2"


def test_reacquire_after_release(backend, events):
    backend.acquire("job", "w1", 30)
    backend.release("job", "w1")
    assert backend.acquire("job", "w2", 30) is True
    assert len(events) == 3


def test_sink_receives_all_fields(backend, events):
    backend.acquire("k", "owner", 45)
    ev = events[0]
    assert set(ev.keys()) == {"event", "key", "owner", "ttl", "success"}
