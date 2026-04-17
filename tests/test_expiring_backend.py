"""Tests for ExpiringBackend."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from schedlock.backends.expiring_backend import ExpiringBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture()
def inner():
    return MemoryBackend()


@pytest.fixture()
def backend(inner):
    return ExpiringBackend(inner, max_age=5.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        ExpiringBackend("not-a-backend")


def test_requires_positive_max_age(inner):
    with pytest.raises(ValueError):
        ExpiringBackend(inner, max_age=0)
    with pytest.raises(ValueError):
        ExpiringBackend(inner, max_age=-1)


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_acquire_success(backend):
    assert backend.acquire("job", "owner-1", 60) is True


def test_acquire_blocked_while_valid(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.acquire("job", "owner-2", 60) is False


def test_acquire_allowed_after_max_age(inner):
    backend = ExpiringBackend(inner, max_age=0.05)
    backend.acquire("job", "owner-1", 60)
    time.sleep(0.1)
    # After max_age, eviction should release the lock
    assert backend.acquire("job", "owner-2", 60) is True


def test_is_locked_evicts_expired(inner):
    backend = ExpiringBackend(inner, max_age=0.05)
    backend.acquire("job", "owner-1", 60)
    time.sleep(0.1)
    assert backend.is_locked("job") is False


def test_release_clears_registry(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.release("job", "owner-1") is True
    assert "job" not in backend._registry


def test_release_wrong_owner_does_not_clear(backend):
    backend.acquire("job", "owner-1", 60)
    result = backend.release("job", "wrong-owner")
    assert result is False
    assert "job" in backend._registry


def test_refresh_delegates_to_inner(inner, backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.refresh("job", "owner-1", 120) is True


def test_no_eviction_before_max_age(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.is_locked("job") is True
