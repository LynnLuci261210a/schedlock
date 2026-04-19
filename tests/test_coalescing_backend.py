"""Tests for CoalescingBackend."""

import time
import pytest
from unittest.mock import MagicMock

from schedlock.backends.coalescing_backend import CoalescingBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=["acquire", "release", "is_locked", "refresh",
                           "__class__"])


@pytest.fixture
def backend():
    return CoalescingBackend(MemoryBackend(), window=0.5)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        CoalescingBackend("not-a-backend")


def test_requires_positive_window():
    with pytest.raises(ValueError):
        CoalescingBackend(MemoryBackend(), window=0)
    with pytest.raises(ValueError):
        CoalescingBackend(MemoryBackend(), window=-1)


def test_inner_property():
    mem = MemoryBackend()
    b = CoalescingBackend(mem, window=1.0)
    assert b.inner is mem


def test_window_property():
    b = CoalescingBackend(MemoryBackend(), window=2.5)
    assert b.window == 2.5


def test_acquire_succeeds_first_time(backend):
    assert backend.acquire("job", "owner-1", 60) is True


def test_acquire_coalesced_within_window(backend):
    backend.acquire("job", "owner-1", 60)
    # Second attempt within window should be coalesced (return False)
    result = backend.acquire("job", "owner-2", 60)
    assert result is False


def test_acquire_allowed_after_window_expires():
    b = CoalescingBackend(MemoryBackend(), window=0.05)
    assert b.acquire("job", "owner-1", 60) is True
    time.sleep(0.1)
    # Release so inner backend allows re-acquire
    b.release("job", "owner-1")
    assert b.acquire("job", "owner-2", 60) is True


def test_release_clears_coalesce_state(backend):
    backend.acquire("job", "owner-1", 60)
    backend.release("job", "owner-1")
    # After release coalesce state is cleared; inner also free
    assert backend.acquire("job", "owner-2", 60) is True


def test_different_keys_not_coalesced(backend):
    assert backend.acquire("job-a", "owner-1", 60) is True
    assert backend.acquire("job-b", "owner-1", 60) is True


def test_is_locked_delegates(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.is_locked("job") is True


def test_is_locked_false_when_not_acquired(backend):
    assert backend.is_locked("job") is False


def test_refresh_delegates(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.refresh("job", "owner-1", 120) is True


def test_refresh_fails_without_acquire(backend):
    # Refreshing a lock that was never acquired should return False
    assert backend.refresh("job", "owner-1", 120) is False
