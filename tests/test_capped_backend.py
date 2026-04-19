"""Tests for CappedBackend."""
import pytest
from unittest.mock import MagicMock

from schedlock.backends.capped_backend import CappedBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    return MemoryBackend()


@pytest.fixture
def backend(inner):
    return CappedBackend(inner, max_holders=2)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        CappedBackend("not-a-backend")


def test_requires_positive_max_holders(inner):
    with pytest.raises(ValueError):
        CappedBackend(inner, max_holders=0)
    with pytest.raises(ValueError):
        CappedBackend(inner, max_holders=-1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_max_holders_property(backend):
    assert backend.max_holders == 2


def test_acquire_succeeds_within_cap(backend):
    assert backend.acquire("job", "owner-1", 60) is True
    assert backend.acquire("job", "owner-2", 60) is True


def test_acquire_blocked_at_cap(backend):
    backend.acquire("job", "owner-1", 60)
    backend.acquire("job", "owner-2", 60)
    assert backend.acquire("job", "owner-3", 60) is False


def test_release_frees_slot(backend):
    backend.acquire("job", "owner-1", 60)
    backend.acquire("job", "owner-2", 60)
    backend.release("job", "owner-1")
    assert backend.acquire("job", "owner-3", 60) is True


def test_same_owner_idempotent(backend):
    backend.acquire("job", "owner-1", 60)
    # Re-acquiring same owner should not consume extra slot
    assert backend.acquire("job", "owner-1", 60) is True
    assert backend.acquire("job", "owner-2", 60) is True
    # Only 2 distinct owners held, cap=2, owner-3 should be blocked
    assert backend.acquire("job", "owner-3", 60) is False


def test_default_max_holders_is_one(inner):
    b = CappedBackend(inner)
    assert b.max_holders == 1
    assert b.acquire("job", "a", 30) is True
    assert b.acquire("job", "b", 30) is False


def test_is_locked_delegates(backend):
    assert backend.is_locked("job") is False
    backend.acquire("job", "owner-1", 60)
    assert backend.is_locked("job") is True


def test_different_keys_independent(backend):
    backend.acquire("job-a", "owner-1", 60)
    backend.acquire("job-a", "owner-2", 60)
    # job-b cap not reached
    assert backend.acquire("job-b", "owner-3", 60) is True
