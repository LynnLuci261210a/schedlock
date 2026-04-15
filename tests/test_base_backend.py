"""Tests for the BaseBackend abstract class."""

import pytest
from schedlock.backends.base import BaseBackend


class ConcreteBackend(BaseBackend):
    """Minimal concrete implementation for testing the base class contract."""

    def __init__(self):
        self._locks = {}

    def acquire(self, job_name, ttl, owner=None):
        if job_name in self._locks:
            return False
        self._locks[job_name] = owner or "default-owner"
        return True

    def release(self, job_name, owner):
        if self._locks.get(job_name) == owner:
            del self._locks[job_name]
            return True
        return False

    def is_locked(self, job_name):
        return job_name in self._locks


class IncompleteBackend(BaseBackend):
    """Incomplete implementation missing required abstract methods."""
    pass


@pytest.fixture
def backend():
    return ConcreteBackend()


def test_cannot_instantiate_abstract_base():
    with pytest.raises(TypeError):
        BaseBackend()


def test_cannot_instantiate_incomplete_subclass():
    with pytest.raises(TypeError):
        IncompleteBackend()


def test_concrete_subclass_instantiates(backend):
    assert isinstance(backend, BaseBackend)


def test_acquire_returns_true_when_unlocked(backend):
    assert backend.acquire("my_job", ttl=60) is True


def test_acquire_returns_false_when_already_locked(backend):
    backend.acquire("my_job", ttl=60, owner="owner-1")
    assert backend.acquire("my_job", ttl=60, owner="owner-2") is False


def test_release_returns_true_for_correct_owner(backend):
    backend.acquire("my_job", ttl=60, owner="owner-1")
    assert backend.release("my_job", owner="owner-1") is True


def test_release_returns_false_for_wrong_owner(backend):
    backend.acquire("my_job", ttl=60, owner="owner-1")
    assert backend.release("my_job", owner="owner-2") is False


def test_is_locked_returns_false_initially(backend):
    assert backend.is_locked("my_job") is False


def test_is_locked_returns_true_after_acquire(backend):
    backend.acquire("my_job", ttl=60, owner="owner-1")
    assert backend.is_locked("my_job") is True


def test_is_locked_returns_false_after_release(backend):
    backend.acquire("my_job", ttl=60, owner="owner-1")
    backend.release("my_job", owner="owner-1")
    assert backend.is_locked("my_job") is False


def test_repr_includes_class_name(backend):
    assert "ConcreteBackend" in repr(backend)
