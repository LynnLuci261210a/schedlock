"""Verify SemaphoreBackend is accessible via the backends package."""
import pytest

from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.semaphore_backend import SemaphoreBackend


def test_semaphore_backend_importable():
    assert SemaphoreBackend is not None


def test_semaphore_backend_is_base_subclass():
    assert issubclass(SemaphoreBackend, BaseBackend)


def test_semaphore_backend_instantiable():
    mem = MemoryBackend()
    sem = SemaphoreBackend(mem, max_holders=2)
    assert isinstance(sem, SemaphoreBackend)


def test_semaphore_backend_has_expected_attrs():
    mem = MemoryBackend()
    sem = SemaphoreBackend(mem, max_holders=3)
    assert hasattr(sem, "inner")
    assert hasattr(sem, "max_holders")
    assert hasattr(sem, "slots_available")
    assert hasattr(sem, "current_holders")
    assert sem.max_holders == 3


def test_semaphore_default_max_holders():
    mem = MemoryBackend()
    sem = SemaphoreBackend(mem)
    assert sem.max_holders == 1
