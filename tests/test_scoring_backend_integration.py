import pytest
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.scoring_backend import ScoringBackend
from schedlock.backends.namespaced_backend import NamespacedBackend


@pytest.fixture
def memory():
    return MemoryBackend()


def test_full_lifecycle_with_high_score(memory):
    b = ScoringBackend(memory, score_fn=lambda k, o: 1.0, min_score=0.5)
    assert b.acquire("job", "worker-1", 60) is True
    assert b.is_locked("job") is True
    assert b.release("job", "worker-1") is True
    assert b.is_locked("job") is False


def test_low_score_prevents_acquire(memory):
    b = ScoringBackend(memory, score_fn=lambda k, o: 0.0, min_score=0.5)
    assert b.acquire("job", "worker-1", 60) is False
    assert b.is_locked("job") is False


def test_score_exactly_at_min_allowed(memory):
    b = ScoringBackend(memory, score_fn=lambda k, o: 0.5, min_score=0.5)
    assert b.acquire("job", "worker-1", 60) is True


def test_dynamic_score_fn(memory):
    scores = {"job": 0.0}
    b = ScoringBackend(memory, score_fn=lambda k, o: scores[k], min_score=0.5)
    assert b.acquire("job", "worker-1", 60) is False
    scores["job"] = 1.0
    assert b.acquire("job", "worker-1", 60) is True


def test_scoring_over_namespaced(memory):
    ns = NamespacedBackend(memory, namespace="prod")
    b = ScoringBackend(ns, score_fn=lambda k, o: 0.9, min_score=0.5)
    assert b.acquire("task", "w1", 30) is True
    assert b.is_locked("task") is True
    assert b.release("task", "w1") is True
