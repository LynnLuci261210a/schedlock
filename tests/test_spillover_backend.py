import pytest
from unittest.mock import MagicMock
from schedlock.backends.base import BaseBackend
from schedlock.backends.spillover_backend import SpilloverBackend
from schedlock.backends.memory_backend import MemoryBackend


def make_backend(acquire_result=True, release_result=True, is_locked_result=False):
    b = MagicMock(spec=BaseBackend)
    b.acquire.return_value = acquire_result
    b.release.return_value = release_result
    b.is_locked.return_value = is_locked_result
    b.refresh.return_value = acquire_result
    return b


@pytest.fixture
def primary():
    return make_backend()


@pytest.fixture
def secondary():
    return make_backend()


@pytest.fixture
def backend(primary, secondary):
    return SpilloverBackend(primary, secondary, threshold=2)


def test_requires_primary_backend(secondary):
    with pytest.raises(TypeError, match="primary must be a BaseBackend instance"):
        SpilloverBackend("not-a-backend", secondary, threshold=1)


def test_requires_secondary_backend(primary):
    with pytest.raises(TypeError, match="secondary must be a BaseBackend instance"):
        SpilloverBackend(primary, "not-a-backend", threshold=1)


def test_requires_positive_threshold(primary, secondary):
    with pytest.raises(ValueError, match="threshold must be a positive integer"):
        SpilloverBackend(primary, secondary, threshold=0)


def test_requires_integer_threshold(primary, secondary):
    with pytest.raises(ValueError, match="threshold must be a positive integer"):
        SpilloverBackend(primary, secondary, threshold=1.5)


def test_primary_property(backend, primary):
    assert backend.primary is primary


def test_secondary_property(backend, secondary):
    assert backend.secondary is secondary


def test_threshold_property(backend):
    assert backend.threshold == 2


def test_acquire_uses_primary_when_below_threshold(backend, primary, secondary):
    result = backend.acquire("job", "worker-1", 30)
    assert result is True
    primary.acquire.assert_called_once_with("job", "worker-1", 30)
    secondary.acquire.assert_not_called()


def test_acquire_spills_to_secondary_when_threshold_reached(backend, primary, secondary):
    backend.acquire("job", "worker-1", 30)
    backend.acquire("job2", "worker-2", 30)
    # Now at threshold=2, next goes to secondary
    backend.acquire("job3", "worker-3", 30)
    assert secondary.acquire.call_count == 1
    secondary.acquire.assert_called_once_with("job3", "worker-3", 30)


def test_release_delegates_to_primary_slot(backend, primary, secondary):
    backend.acquire("job", "worker-1", 30)
    backend.release("job", "worker-1")
    primary.release.assert_called_once_with("job", "worker-1")
    secondary.release.assert_not_called()


def test_release_delegates_to_secondary_slot(backend, primary, secondary):
    # Fill primary to threshold
    backend.acquire("job", "w1", 30)
    backend.acquire("job2", "w2", 30)
    # Spill to secondary
    backend.acquire("job3", "w3", 30)
    backend.release("job3", "w3")
    secondary.release.assert_called_once_with("job3", "w3")


def test_release_decrements_primary_count(backend, primary):
    primary.release.return_value = True
    backend.acquire("job", "w1", 30)
    backend.acquire("job2", "w2", 30)
    assert backend._primary_count == 2
    backend.release("job", "w1")
    assert backend._primary_count == 1


def test_is_locked_checks_both_backends(backend, primary, secondary):
    primary.is_locked.return_value = False
    secondary.is_locked.return_value = True
    assert backend.is_locked("job") is True


def test_refresh_delegates_to_correct_slot(backend, primary, secondary):
    backend.acquire("job", "w1", 30)
    backend.refresh("job", "w1", 60)
    primary.refresh.assert_called_once_with("job", "w1", 60)
    secondary.refresh.assert_not_called()


def test_full_lifecycle_over_memory():
    p = MemoryBackend()
    s = MemoryBackend()
    b = SpilloverBackend(p, s, threshold=1)
    assert b.acquire("job", "w1", 30) is True   # goes to primary
    assert b.acquire("job2", "w2", 30) is True  # spills to secondary
    assert p.is_locked("job") is True
    assert s.is_locked("job2") is True
    assert b.release("job", "w1") is True
    assert p.is_locked("job") is False
