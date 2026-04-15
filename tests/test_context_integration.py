"""Integration tests for LockContext with MemoryBackend."""

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.context import LockContext


@pytest.fixture
def backend():
    return MemoryBackend()


def test_full_lifecycle_via_context_manager(backend):
    with LockContext(backend, "integration-job", ttl=60, owner="proc-1") as acquired:
        assert acquired is True
        assert backend.is_locked("integration-job") is True
    assert backend.is_locked("integration-job") is False


def test_second_worker_blocked_while_first_holds_lock(backend):
    with LockContext(backend, "shared-job", ttl=60, owner="proc-1") as first:
        assert first is True
        with LockContext(backend, "shared-job", ttl=60, owner="proc-2") as second:
            assert second is False


def test_lock_reacquirable_after_release(backend):
    with LockContext(backend, "job", owner="proc-1"):
        pass
    with LockContext(backend, "job", owner="proc-2") as acquired:
        assert acquired is True


def test_backend_lock_helper_matches_lock_context(backend):
    """backend.lock() should behave identically to LockContext directly."""
    with backend.lock("helper-job", ttl=30, owner="w") as acquired:
        assert acquired is True
        assert backend.is_locked("helper-job") is True
    assert backend.is_locked("helper-job") is False


def test_exception_inside_context_releases_lock(backend):
    try:
        with LockContext(backend, "crash-job", owner="proc-1"):
            raise RuntimeError("simulated failure")
    except RuntimeError:
        pass
    assert backend.is_locked("crash-job") is False
