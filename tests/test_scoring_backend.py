import pytest
from unittest.mock import MagicMock
from schedlock.backends.scoring_backend import ScoringBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock()


@pytest.fixture
def backend(inner):
    return ScoringBackend(inner, score_fn=lambda k, o: 0.8, min_score=0.5)


def test_requires_inner_backend():
    with pytest.raises(ValueError):
        ScoringBackend(None, score_fn=lambda k, o: 1.0)


def test_requires_callable_score_fn(inner):
    with pytest.raises(TypeError):
        ScoringBackend(inner, score_fn="not_callable")


def test_requires_numeric_min_score(inner):
    with pytest.raises(TypeError):
        ScoringBackend(inner, score_fn=lambda k, o: 1.0, min_score="high")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_min_score_property(backend):
    assert backend.min_score == 0.5


def test_acquire_delegates_when_score_sufficient(inner, backend):
    inner.acquire.return_value = True
    result = backend.acquire("job", "worker-1", 60)
    assert result is True
    inner.acquire.assert_called_once_with("job", "worker-1", 60)


def test_acquire_blocked_when_score_below_min(inner):
    b = ScoringBackend(inner, score_fn=lambda k, o: 0.2, min_score=0.5)
    result = b.acquire("job", "worker-1", 60)
    assert result is False
    inner.acquire.assert_not_called()


def test_last_score_recorded_after_acquire(inner):
    b = ScoringBackend(inner, score_fn=lambda k, o: 0.9, min_score=0.5)
    inner.acquire.return_value = True
    b.acquire("job", "worker-1", 60)
    assert b.last_score("job") == pytest.approx(0.9)


def test_last_score_recorded_even_when_blocked(inner):
    b = ScoringBackend(inner, score_fn=lambda k, o: 0.1, min_score=0.5)
    b.acquire("job", "worker-1", 60)
    assert b.last_score("job") == pytest.approx(0.1)


def test_last_score_none_for_unknown_key(backend):
    assert backend.last_score("unknown") is None


def test_release_delegates(inner, backend):
    inner.release.return_value = True
    assert backend.release("job", "worker-1") is True
    inner.release.assert_called_once_with("job", "worker-1")


def test_is_locked_delegates(inner, backend):
    inner.is_locked.return_value = True
    assert backend.is_locked("job") is True


def test_refresh_delegates(inner, backend):
    inner.refresh.return_value = True
    assert backend.refresh("job", "worker-1", 30) is True
    inner.refresh.assert_called_once_with("job", "worker-1", 30)


def test_score_fn_receives_key_and_owner(inner):
    calls = []
    def score_fn(k, o):
        calls.append((k, o))
        return 1.0
    b = ScoringBackend(inner, score_fn=score_fn)
    inner.acquire.return_value = True
    b.acquire("myjob", "ownerX", 60)
    assert calls == [("myjob", "ownerX")]
