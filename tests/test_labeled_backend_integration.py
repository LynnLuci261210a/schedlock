import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.labeled_backend import LabeledBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle(memory):
    backend = LabeledBackend(memory, {"env": "test"})
    assert backend.acquire("job", "w1", 60) is True
    assert backend.is_locked("job") is True
    assert backend.release("job", "w1") is True
    assert backend.is_locked("job") is False


def test_second_worker_blocked(memory):
    backend = LabeledBackend(memory, {"env": "test"})
    assert backend.acquire("job", "w1", 60) is True
    assert backend.acquire("job", "w2", 60) is False


def test_labels_do_not_affect_locking(memory):
    b1 = LabeledBackend(memory, {"env": "prod"})
    b2 = LabeledBackend(memory, {"env": "staging"})
    # Both wrap the same memory backend — lock is shared
    assert b1.acquire("job", "w1", 60) is True
    assert b2.acquire("job", "w2", 60) is False


def test_labeled_over_namespaced():
    memory = MemoryBackend()
    ns = NamespacedBackend(memory, "myapp")
    backend = LabeledBackend(ns, {"team": "infra", "env": "prod"})
    assert backend.acquire("deploy", "w1", 30) is True
    assert backend.is_locked("deploy") is True
    assert backend.get_label("team") == "infra"
    assert backend.release("deploy", "w1") is True
    assert backend.is_locked("deploy") is False


def test_refresh_extends_lock(memory):
    backend = LabeledBackend(memory, {"env": "test"})
    assert backend.acquire("job", "w1", 60) is True
    assert backend.refresh("job", "w1", 120) is True
    assert backend.is_locked("job") is True
