"""Tests for ClaimingBackend."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.claiming_backend import ClaimingBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture()
def inner():
    return MemoryBackend()


@pytest.fixture()
def backend(inner):
    return ClaimingBackend(inner, max_claim_seconds=5.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        ClaimingBackend(inner="not-a-backend")


def test_requires_numeric_max_claim():
    inner = MemoryBackend()
    with pytest.raises(TypeError, match="max_claim_seconds must be a numeric value"):
        ClaimingBackend(inner, max_claim_seconds="5")


def test_requires_positive_max_claim():
    inner = MemoryBackend()
    with pytest.raises(ValueError, match="max_claim_seconds must be positive"):
        ClaimingBackend(inner, max_claim_seconds=0)


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_max_claim_seconds_property(backend):
    assert backend.max_claim_seconds == 5.0


def test_acquire_success(backend):
    assert backend.acquire("job", "worker-1") is True


def test_acquire_blocked_by_other_owner(backend):
    backend.acquire("job", "worker-1")
    assert backend.acquire("job", "worker-2") is False


def test_release_clears_claim(backend):
    backend.acquire("job", "worker-1")
    backend.release("job", "worker-1")
    assert backend.claim_age("job", "worker-1") is None


def test_claim_age_returns_none_before_acquire(backend):
    assert backend.claim_age("job", "worker-1") is None


def test_claim_age_returns_elapsed_after_acquire(backend):
    backend.acquire("job", "worker-1")
    age = backend.claim_age("job", "worker-1")
    assert age is not None
    assert 0.0 <= age < 1.0


def test_overdue_claim_blocks_reacquire(inner):
    backend = ClaimingBackend(inner, max_claim_seconds=0.05)
    backend.acquire("job", "worker-1")
    # Release from inner directly so the claim record stays in place
    inner.release("job", "worker-1")
    time.sleep(0.1)
    assert backend.acquire("job", "worker-1") is False


def test_release_after_overdue_allows_fresh_acquire(inner):
    backend = ClaimingBackend(inner, max_claim_seconds=0.05)
    backend.acquire("job", "worker-1")
    time.sleep(0.1)
    # Proper release clears the claim record
    backend.release("job", "worker-1")
    assert backend.acquire("job", "worker-1") is True


def test_is_locked_delegates(backend):
    assert backend.is_locked("job") is False
    backend.acquire("job", "worker-1")
    assert backend.is_locked("job") is True


def test_refresh_delegates(backend):
    backend.acquire("job", "worker-1")
    assert backend.refresh("job", "worker-1", ttl=10) is True


def test_independent_keys_tracked_separately(backend):
    assert backend.acquire("job-a", "worker-1") is True
    assert backend.acquire("job-b", "worker-1") is True
    assert backend.claim_age("job-a", "worker-1") is not None
    assert backend.claim_age("job-b", "worker-1") is not None
