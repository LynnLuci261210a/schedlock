"""Verify PausedBackend is exported from the backends package."""

from schedlock.backends.paused_backend import PausedBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


def test_paused_backend_importable():
    assert PausedBackend is not None


def test_paused_backend_is_base_subclass():
    assert issubclass(PausedBackend, BaseBackend)


def test_paused_backend_instantiable():
    b = PausedBackend(MemoryBackend())
    assert isinstance(b, PausedBackend)


def test_paused_backend_has_expected_attrs():
    b = PausedBackend(MemoryBackend())
    assert hasattr(b, "pause")
    assert hasattr(b, "resume")
    assert hasattr(b, "is_paused")
    assert hasattr(b, "inner")
    assert callable(b.pause)
    assert callable(b.resume)
