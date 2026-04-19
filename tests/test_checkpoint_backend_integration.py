import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.checkpoint_backend import CheckpointBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_checkpoints(memory):
    backend = CheckpointBackend(memory)
    assert backend.acquire("report", "worker-1", 60)
    cp = backend.checkpoint_for("report")
    assert cp["event"] == "acquired"
    assert cp["owner"] == "worker-1"

    assert backend.release("report", "worker-1")
    cp = backend.checkpoint_for("report")
    assert cp["event"] == "released"


def test_second_worker_blocked_does_not_overwrite_checkpoint(memory):
    backend = CheckpointBackend(memory)
    backend.acquire("job", "w1", 60)
    backend.acquire("job", "w2", 60)  # blocked
    cp = backend.checkpoint_for("job")
    assert cp["owner"] == "w1"


def test_reacquire_after_release_updates_checkpoint(memory):
    backend = CheckpointBackend(memory)
    backend.acquire("job", "w1", 60)
    backend.release("job", "w1")
    backend.acquire("job", "w2", 30)
    cp = backend.checkpoint_for("job")
    assert cp["event"] == "acquired"
    assert cp["owner"] == "w2"
    assert cp["ttl"] == 30


def test_checkpoint_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="ns1")
    backend = CheckpointBackend(ns)
    assert backend.acquire("task", "w1", 10)
    assert backend.checkpoint_for("task")["event"] == "acquired"
    assert backend.release("task", "w1")
    assert backend.checkpoint_for("task")["event"] == "released"
