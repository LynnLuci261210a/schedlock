"""Tests for OnceBackend."""

import pytest
from unittest.mock import MagicMock
from schedlock.backends.once_backend import OnceBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.base import BaseBackend


@pytest.fixture
def inner():
    return MemoryBackend()


@pytest.fixture
def backend(inner):
    return OnceBackend(inner)


def test_requires_inner_backend():
    with pytest.raises(TypeError):
        OnceBackend("not-a-backend")


def test_inner_property(backend, inner):
    assert backend.inner is inner


def test_acquire_succeeds_first_time(backend):
    assert backend.acquire("job", "owner-1", 60) is True


def test_acquire_blocked_while_held(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.acquire("job", "owner-2", 60) is False


def test_acquire_permanently_blocked_after_release(backend):
    backend.acquire("job", "owner-1", 60)
    backend.release("job", "owner-1")
    assert backend.acquire("job", "owner-2", 60) is False


def test_release_returns_true_on_success(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.release("job", "owner-1") is True


def test_release_marks_key_as_used(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.ever_acquired("job") is False
    backend.release("job", "owner-1")
    assert backend.ever_acquired("job") is True


def test_ever_acquired_false_before_any_release(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.ever_acquired("job") is False


def test_ever_acquired_false_for_unknown_key(backend):
    assert backend.ever_acquired("never-seen") is False


def test_different_keys_are_independent(backend):
    backend.acquire("job-a", "owner-1", 60)
    backend.release("job-a", "owner-1")
    # job-b was never used, so it should still be acquirable
    assert backend.acquire("job-b", "owner-1", 60) is True


def test_is_locked_delegates_to_inner(backend):
    assert backend.is_locked("job") is False
    backend.acquire("job", "owner-1", 60)
    assert backend.is_locked("job") is True


def test_refresh_delegates_to_inner(backend):
    backend.acquire("job", "owner-1", 60)
    assert backend.refresh("job", "owner-1", 120) is True


def test_release_by_wrong_owner_does_not_mark_used(backend):
    backend.acquire("job", "owner-1", 60)
    result = backend.release("job", "wrong-owner")
    assert result is False
    assert backend.ever_acquired("job") is False
    # original owner can still release
    assert backend.release("job", "owner-1") is True
