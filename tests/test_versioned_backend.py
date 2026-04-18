import pytest
from unittest.mock import MagicMock
from schedlock.backends.versioned_backend import VersionedBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


@pytest.fixture
def inner():
    return MagicMock(spec=BaseBackend)


@pytest.fixture
def backend(inner):
    return VersionedBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        VersionedBackend("not-a-backend")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_initial_version_is_zero(backend):
    assert backend.version_of("my-job") == 0


def test_acquire_success_increments_version(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("my-job", "worker-1", 60)
    assert backend.version_of("my-job") == 1


def test_acquire_failure_does_not_increment(backend, inner):
    inner.acquire.return_value = False
    backend.acquire("my-job", "worker-1", 60)
    assert backend.version_of("my-job") == 0


def test_version_increments_on_each_acquire(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("my-job", "worker-1", 60)
    backend.acquire("my-job", "worker-1", 60)
    backend.acquire("my-job", "worker-1", 60)
    assert backend.version_of("my-job") == 3


def test_versions_are_per_key(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("job-a", "w", 60)
    backend.acquire("job-a", "w", 60)
    backend.acquire("job-b", "w", 60)
    assert backend.version_of("job-a") == 2
    assert backend.version_of("job-b") == 1


def test_release_delegates(backend, inner):
    inner.release.return_value = True
    assert backend.release("my-job", "worker-1") is True
    inner.release.assert_called_once_with("my-job", "worker-1")


def test_is_locked_delegates(backend, inner):
    inner.is_locked.return_value = True
    assert backend.is_locked("my-job") is True


def test_refresh_delegates(backend, inner):
    inner.refresh.return_value = True
    assert backend.refresh("my-job", "worker-1", 30) is True


def test_reset_version(backend, inner):
    inner.acquire.return_value = True
    backend.acquire("my-job", "w", 60)
    assert backend.version_of("my-job") == 1
    backend.reset_version("my-job")
    assert backend.version_of("my-job") == 0


def test_reset_version_unknown_key_is_safe(backend):
    backend.reset_version("nonexistent")
    assert backend.version_of("nonexistent") == 0
