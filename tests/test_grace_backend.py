"""Tests for GraceBackend."""

from __future__ import annotations

import time
import pytest
from unittest.mock import MagicMock

from schedlock.backends.base import BaseBackend
from schedlock.backends.grace_backend import GraceBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MemoryBackend()


@pytest.fixture
def backend(inner):
    return GraceBackend(inner, grace_seconds=1.0)


def test_requires_inner_backend():
    with pytest.raises(TypeError, match="inner must be a BaseBackend instance"):
        GraceBackend(object(), grace_seconds=1.0)


def test_requires_positive_grace_seconds(inner):
    with pytest.raises(ValueError, match="grace_seconds must be a positive number"):
        GraceBackend(inner, grace_seconds=0)


def test_requires_numeric_grace_seconds(inner):
    with pytest.raises(ValueError, match="grace_seconds must be a positive number"):
        GraceBackend(inner, grace_seconds="fast")


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_grace_seconds_property(backend):
    assert backend.grace_seconds == 1.0


def test_acquire_succeeds_initially(backend):
    assert backend.acquire("job", "worker-1") is True


def test_acquire_blocked_during_grace_after_release(backend):
    backend.acquire("job", "worker-1")
    backend.release("job", "worker-1")
    # Immediately after release, grace period is active
    assert backend.acquire("job", "worker-2") is False


def test_acquire_succeeds_after_grace_expires(inner):
    b = GraceBackend(inner, grace_seconds=0.05)
    b.acquire("job", "worker-1")
    b.release("job", "worker-1")
    time.sleep(0.1)
    assert b.acquire("job", "worker-2") is True


def test_release_returns_false_if_not_owner(backend):
    backend.acquire("job", "worker-1")
    assert backend.release("job", "worker-2") is False


def test_is_locked_true_during_grace(backend):
    backend.acquire("job", "worker-1")
    backend.release("job", "worker-1")
    assert backend.is_locked("job") is True


def test_is_locked_false_after_grace_expires(inner):
    b = GraceBackend(inner, grace_seconds=0.05)
    b.acquire("job", "worker-1")
    b.release("job", "worker-1")
    time.sleep(0.1)
    assert b.is_locked("job") is False


def test_release_without_acquire_does_not_set_grace(backend):
    released = backend.release("job", "worker-1")
    assert released is False
    # No grace should be set
    assert backend.acquire("job", "worker-1") is True


def test_refresh_delegates_to_inner(backend):
    backend.acquire("job", "worker-1")
    assert backend.refresh("job", "worker-1", ttl=30) is True


def test_independent_keys_have_independent_grace(inner):
    b = GraceBackend(inner, grace_seconds=1.0)
    b.acquire("job-a", "worker-1")
    b.release("job-a", "worker-1")
    # job-b should not be affected
    assert b.acquire("job-b", "worker-2") is True
