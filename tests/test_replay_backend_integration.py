import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.replay_backend import ReplayBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_recorded(memory):
    backend = ReplayBackend(memory)
    assert backend.acquire("job", "w1", 60) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "w1") is True
    assert backend.is_locked("job") is False
    log = backend.log
    assert log[0].operation == "acquire"
    assert log[1].operation == "release"


def test_second_worker_blocked_is_recorded(memory):
    backend = ReplayBackend(memory)
    backend.acquire("job", "w1", 60)
    result = backend.acquire("job", "w2", 60)
    assert result is False
    failed = [e for e in backend.log if not e.result]
    assert len(failed) == 1
    assert failed[0].owner == "w2"


def test_replay_onto_fresh_backend(memory):
    backend = ReplayBackend(memory)
    backend.acquire("job", "w1", 60)
    backend.release("job", "w1")

    fresh = MemoryBackend()
    replayed = backend.replay_onto(fresh)
    assert replayed[0].result is True
    assert replayed[1].result is True


def test_replay_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    backend = ReplayBackend(ns)
    backend.acquire("job", "w1", 60)
    assert backend.is_locked("job") is True
    backend.release("job", "w1")
    assert len(backend.log) == 2


def test_clear_and_rerecord(memory):
    backend = ReplayBackend(memory)
    backend.acquire("job", "w1", 60)
    backend.clear_log()
    assert backend.log == []
    backend.release("job", "w1")
    assert len(backend.log) == 1
    assert backend.log[0].operation == "release"
