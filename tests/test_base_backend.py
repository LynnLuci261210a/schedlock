"""Tests for BaseBackend abstract class and its .lock() helper."""

import pytest
from unittest.mock import patch

from schedlock.backends.base import BaseBackend
from schedlock.context import LockContext


class ConcreteBackend(BaseBackend):
    """Minimal concrete implementation for testing."""

    def __init__(self):
        self._locks = {}

    def acquire(self, job_name, ttl=300, owner=None):
        if job_name in self._locks:
            return False
        self._locks[job_name] = owner or "anon"
        return True

    def release(self, job_name, owner=None):
        if self._locks.get(job_name) == (owner or "anon"):
            del self._locks[job_name]
            return True
        return False

    def is_locked(self, job_name):
        return job_name in self._locks


@pytest.fixture
def backend():
    return ConcreteBackend()


def test_acquire_returns_true_when_free(backend):
    assert backend.acquire("job", owner="me") is True


def test_acquire_returns_false_when_held(backend):
    backend.acquire("job", owner="me")
    assert backend.acquire("job", owner="other") is False


def test_release_returns_true_for_owner(backend):
    backend.acquire("job", owner="me")
    assert backend.release("job", owner="me") is True


def test_release_returns_false_for_non_owner(backend):
    backend.acquire("job", owner="me")
    assert backend.release("job", owner="other") is False


def test_is_locked_reflects_state(backend):
    assert backend.is_locked("job") is False
    backend.acquire("job", owner="me")
    assert backend.is_locked("job") is True


def test_lock_returns_lock_context(backend):
    ctx = backend.lock("my-job", ttl=30, owner="worker")
    assert isinstance(ctx, LockContext)
    assert ctx.job_name == "my-job"
    assert ctx.ttl == 30
    assert ctx.owner == "worker"


def test_lock_context_manager_acquires_and_releases(backend):
    with backend.lock("my-job", owner="w1") as acquired:
        assert acquired is True
        assert backend.is_locked("my-job") is True
    assert backend.is_locked("my-job") is False


def test_lock_context_manager_blocked(backend):
    backend.acquire("my-job", owner="other")
    with backend.lock("my-job", owner="w1") as acquired:
        assert acquired is False


def test_repr_contains_class_name(backend):
    assert "ConcreteBackend" in repr(backend)


def test_cannot_instantiate_base_directly():
    with pytest.raises(TypeError):
        BaseBackend()
