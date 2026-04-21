import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.weight_backend import WeightBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture()
def inner():
    return MemoryBackend()


@pytest.fixture()
def backend(inner):
    return WeightBackend(inner, weight_fn=lambda key, owner: 5.0, min_weight=1.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend"):
        WeightBackend(object(), weight_fn=lambda k, o: 1.0)


def test_requires_callable_weight_fn(inner):
    with pytest.raises(TypeError, match="weight_fn must be callable"):
        WeightBackend(inner, weight_fn="not_callable")


def test_requires_numeric_min_weight(inner):
    with pytest.raises(TypeError, match="min_weight must be numeric"):
        WeightBackend(inner, weight_fn=lambda k, o: 1.0, min_weight="high")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_min_weight_property(backend):
    assert backend.min_weight == 1.0


def test_acquire_succeeds_when_weight_above_min(inner):
    b = WeightBackend(inner, weight_fn=lambda k, o: 10.0, min_weight=5.0)
    assert b.acquire("job", "worker-1") is True
    assert inner.is_locked("job") is True


def test_acquire_blocked_when_weight_below_min(inner):
    b = WeightBackend(inner, weight_fn=lambda k, o: 0.5, min_weight=1.0)
    assert b.acquire("job", "worker-1") is False
    assert inner.is_locked("job") is False


def test_acquire_blocked_when_weight_equals_min_minus_epsilon(inner):
    b = WeightBackend(inner, weight_fn=lambda k, o: 0.999, min_weight=1.0)
    assert b.acquire("job", "worker-1") is False


def test_acquire_succeeds_when_weight_exactly_at_min(inner):
    b = WeightBackend(inner, weight_fn=lambda k, o: 1.0, min_weight=1.0)
    assert b.acquire("job", "worker-1") is True


def test_last_weight_recorded_on_acquire(inner):
    b = WeightBackend(inner, weight_fn=lambda k, o: 7.3, min_weight=1.0)
    b.acquire("job", "worker-1")
    assert b.last_weight("job") == pytest.approx(7.3)


def test_last_weight_recorded_even_when_blocked(inner):
    b = WeightBackend(inner, weight_fn=lambda k, o: 0.1, min_weight=5.0)
    b.acquire("job", "worker-1")
    assert b.last_weight("job") == pytest.approx(0.1)


def test_last_weight_none_before_any_acquire(backend):
    assert backend.last_weight("never_tried") is None


def test_release_delegates(inner, backend):
    backend.acquire("job", "worker-1")
    assert backend.release("job", "worker-1") is True
    assert inner.is_locked("job") is False


def test_is_locked_delegates(inner, backend):
    assert backend.is_locked("job") is False
    backend.acquire("job", "worker-1")
    assert backend.is_locked("job") is True


def test_dynamic_weight_fn(inner):
    scores = {"job-a": 10.0, "job-b": 0.0}
    b = WeightBackend(inner, weight_fn=lambda k, o: scores.get(k, 0.0), min_weight=5.0)
    assert b.acquire("job-a", "w") is True
    assert b.acquire("job-b", "w") is False
