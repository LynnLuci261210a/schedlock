"""Tests for the in-memory lock backend."""

import time
import pytest

from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def backend():
    return MemoryBackend()


def test_acquire_lock_success(backend):
    result = backend.acquire("my_job", ttl=60, owner="worker-1")
    assert result is True


def test_acquire_lock_blocked_by_existing(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    result = backend.acquire("my_job", ttl=60, owner="worker-2")
    assert result is False


def test_acquire_lock_after_ttl_expiry(backend):
    backend.acquire("my_job", ttl=1, owner="worker-1")
    time.sleep(1.1)
    result = backend.acquire("my_job", ttl=60, owner="worker-2")
    assert result is True


def test_release_lock_by_owner(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    result = backend.release("my_job", owner="worker-1")
    assert result is True


def test_release_lock_wrong_owner(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    result = backend.release("my_job", owner="worker-2")
    assert result is False


def test_release_nonexistent_lock(backend):
    result = backend.release("ghost_job", owner="worker-1")
    assert result is False


def test_is_locked_when_active(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    assert backend.is_locked("my_job") is True


def test_is_locked_when_not_acquired(backend):
    assert backend.is_locked("my_job") is False


def test_is_locked_after_expiry(backend):
    backend.acquire("my_job", ttl=1, owner="worker-1")
    time.sleep(1.1)
    assert backend.is_locked("my_job") is False


def test_is_locked_after_release(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    backend.release("my_job", owner="worker-1")
    assert backend.is_locked("my_job") is False


def test_multiple_independent_jobs(backend):
    assert backend.acquire("job_a", ttl=60, owner="w1") is True
    assert backend.acquire("job_b", ttl=60, owner="w2") is True
    assert backend.is_locked("job_a") is True
    assert backend.is_locked("job_b") is True


def test_repr(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    r = repr(backend)
    assert "MemoryBackend" in r
    assert "my_job" in r
