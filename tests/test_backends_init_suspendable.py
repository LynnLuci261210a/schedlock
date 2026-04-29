"""Verify SuspendableBackend is importable from the backends package."""
from schedlock.backends.suspendable_backend import SuspendableBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


def test_suspendable_backend_importable():
    assert SuspendableBackend is not None


def test_suspendable_backend_is_base_subclass():
    assert issubclass(SuspendableBackend, BaseBackend)


def test_suspendable_backend_instantiable():
    inner = MemoryBackend()
    b = SuspendableBackend(inner)
    assert isinstance(b, SuspendableBackend)


def test_suspendable_backend_has_expected_attrs():
    inner = MemoryBackend()
    b = SuspendableBackend(inner)
    assert hasattr(b, "suspend")
    assert hasattr(b, "resume")
    assert hasattr(b, "is_suspended")
    assert hasattr(b, "inner")
    assert hasattr(b, "reason")
