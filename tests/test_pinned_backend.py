"""Tests for PinnedBackend."""

import pytest

from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.pinned_backend import PinnedBackend


@pytest.fixture
def inner():
    return MemoryBackend()


@pytest.fixture
def backend(inner):
    return PinnedBackend(inner, allowed_owners=["worker-1", "worker-2"])


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        PinnedBackend("not-a-backend", allowed_owners=["worker-1"])


def test_requires_non_empty_allowed_owners(inner):
    with pytest.raises(ValueError):
        PinnedBackend(inner, allowed_owners=[])


def test_inner_property(inner, backend):
    assert backend.inner is inner


def test_allowed_owners_property(backend):
    assert backend.allowed_owners == frozenset({"worker-1", "worker-2"})


def test_acquire_succeeds_for_allowed_owner(backend):
    assert backend.acquire("job", "worker-1", 60) is True


def test_acquire_blocked_for_unknown_owner(backend):
    assert backend.acquire("job", "intruder", 60) is False


def test_acquire_blocked_does_not_set_lock(backend):
    backend.acquire("job", "intruder", 60)
    assert backend.is_locked("job") is False


def test_release_succeeds_for_allowed_owner(backend):
    backend.acquire("job", "worker-1", 60)
    assert backend.release("job", "worker-1") is True


def test_release_blocked_for_unknown_owner(backend, inner):
    inner.acquire("job", "worker-1", 60)
    assert backend.release("job", "intruder") is False
    assert backend.is_locked("job") is True


def test_is_locked_delegates(backend):
    assert backend.is_locked("job") is False
    backend.acquire("job", "worker-1", 60)
    assert backend.is_locked("job") is True


def test_refresh_allowed_owner(backend):
    backend.acquire("job", "worker-1", 60)
    assert backend.refresh("job", "worker-1", 120) is True


def test_refresh_blocked_for_unknown_owner(backend):
    backend.acquire("job", "worker-1", 60)
    assert backend.refresh("job", "intruder", 120) is False


def test_multiple_allowed_owners_can_acquire_different_keys(backend):
    assert backend.acquire("job-a", "worker-1", 60) is True
    assert backend.acquire("job-b", "worker-2", 60) is True
