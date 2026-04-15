import os
import time
import pytest
from unittest.mock import patch
from schedlock.backends.file_backend import FileBackend


@pytest.fixture
def backend(tmp_path):
    return FileBackend(lock_dir=str(tmp_path))


def test_acquire_lock_success(backend):
    acquired = backend.acquire("my_job", ttl=60, owner="worker-1")
    assert acquired is True


def test_acquire_lock_blocked_by_existing(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    acquired = backend.acquire("my_job", ttl=60, owner="worker-2")
    assert acquired is False


def test_acquire_lock_after_ttl_expiry(backend):
    backend.acquire("my_job", ttl=1, owner="worker-1")
    time.sleep(1.1)
    acquired = backend.acquire("my_job", ttl=60, owner="worker-2")
    assert acquired is True


def test_release_lock_by_owner(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    released = backend.release("my_job", owner="worker-1")
    assert released is True


def test_release_lock_by_non_owner(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    released = backend.release("my_job", owner="worker-2")
    assert released is False


def test_release_nonexistent_lock(backend):
    released = backend.release("nonexistent_job", owner="worker-1")
    assert released is False


def test_is_locked_when_active(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    assert backend.is_locked("my_job") is True


def test_is_locked_when_expired(backend):
    backend.acquire("my_job", ttl=1, owner="worker-1")
    time.sleep(1.1)
    assert backend.is_locked("my_job") is False


def test_is_locked_no_lock_file(backend):
    assert backend.is_locked("ghost_job") is False


def test_acquire_after_release(backend):
    backend.acquire("my_job", ttl=60, owner="worker-1")
    backend.release("my_job", owner="worker-1")
    acquired = backend.acquire("my_job", ttl=60, owner="worker-2")
    assert acquired is True


def test_lock_path_sanitizes_slashes(backend):
    path = backend._lock_path("namespace/my_job")
    assert "/" not in os.path.basename(path)
