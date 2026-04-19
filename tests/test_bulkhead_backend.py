"""Tests for BulkheadBackend."""
import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.bulkhead_backend import BulkheadBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MemoryBackend()


@pytest.fixture
def backend(inner):
    return BulkheadBackend(inner, max_concurrent=2)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        BulkheadBackend("not-a-backend")


def test_requires_positive_max_concurrent(inner):
    with pytest.raises(ValueError):
        BulkheadBackend(inner, max_concurrent=0)
    with pytest.raises(ValueError):
        BulkheadBackend(inner, max_concurrent=-1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_concurrent_property(inner):
    b = BulkheadBackend(inner, max_concurrent=3)
    assert b.max_concurrent == 3


def test_acquire_within_limit(backend):
    assert backend.acquire("job:a", "w1", 60) is True
    assert backend.acquire("job:b", "w2", 60) is True
    assert backend.active_count() == 2


def test_acquire_blocked_when_at_capacity(inner):
    b = BulkheadBackend(inner, max_concurrent=1)
    assert b.acquire("job:a", "w1", 60) is True
    assert b.acquire("job:b", "w2", 60) is False


def test_release_frees_slot(inner):
    b = BulkheadBackend(inner, max_concurrent=1)
    b.acquire("job:a", "w1", 60)
    b.release("job:a", "w1")
    assert b.active_count() == 0
    assert b.acquire("job:b", "w2", 60) is True


def test_release_wrong_owner_does_not_free_slot(inner):
    b = BulkheadBackend(inner, max_concurrent=1)
    b.acquire("job:a", "w1", 60)
    result = b.release("job:a", "wrong-owner")
    assert result is False
    assert b.active_count() == 1


def test_is_locked_delegates(backend):
    backend.acquire("job:a", "w1", 60)
    assert backend.is_locked("job:a") is True
    assert backend.is_locked("job:z") is False


def test_refresh_delegates(backend):
    backend.acquire("job:a", "w1", 60)
    assert backend.refresh("job:a", "w1", 120) is True


def test_active_count_starts_at_zero(backend):
    assert backend.active_count() == 0


def test_reacquire_same_key_not_double_counted(inner):
    b = BulkheadBackend(inner, max_concurrent=1)
    b.acquire("job:a", "w1", 60)
    # same key is already active — should still go through (inner decides)
    # inner will block same key different owner, but slot check passes
    result = b.acquire("job:a", "w2", 60)
    assert b.active_count() == 1
