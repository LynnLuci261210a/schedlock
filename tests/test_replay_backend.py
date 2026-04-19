import pytest
from unittest.mock import MagicMock
from schedlock.backends.replay_backend import ReplayBackend, ReplayEntry
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


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
    return ReplayBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        ReplayBackend("not-a-backend")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_delegates_and_records(backend, inner):
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 60)
    assert len(backend.log) == 1
    entry = backend.log[0]
    assert entry.operation == "acquire"
    assert entry.key == "job"
    assert entry.owner == "worker-1"
    assert entry.ttl == 60
    assert entry.result is True


def test_release_delegates_and_records(backend, inner):
    backend.release("job", "worker-1")
    assert len(backend.log) == 1
    entry = backend.log[0]
    assert entry.operation == "release"
    assert entry.ttl is None
    assert entry.result is True


def test_log_accumulates_multiple_entries(backend):
    backend.acquire("job", "w1", 30)
    backend.acquire("job", "w2", 30)
    backend.release("job", "w1")
    assert len(backend.log) == 3


def test_log_returns_copy(backend):
    backend.acquire("job", "w1", 30)
    log1 = backend.log
    log1.clear()
    assert len(backend.log) == 1


def test_clear_log(backend):
    backend.acquire("job", "w1", 30)
    backend.clear_log()
    assert backend.log == []


def test_is_locked_delegates(backend, inner):
    backend.is_locked("job")
    inner.is_locked.assert_called_once_with("job")


def test_refresh_delegates(backend, inner):
    backend.refresh("job", "w1", 60)
    inner.refresh.assert_called_once_with("job", "w1", 60)


def test_replay_onto_target():
    source_inner = MemoryBackend()
    source = ReplayBackend(source_inner)
    source.acquire("job", "w1", 60)
    source.release("job", "w1")

    target = MemoryBackend()
    replayed = source.replay_onto(target)
    assert len(replayed) == 2
    assert replayed[0].operation == "acquire"
    assert replayed[0].result is True
    assert replayed[1].operation == "release"


def test_entry_str():
    e = ReplayEntry("acquire", "job", "w1", 60, True)
    assert "acquire" in str(e)
    assert "job" in str(e)
