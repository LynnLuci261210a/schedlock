"""Verify GatedBackend is exported from the backends package."""
import pytest

from schedlock.backends.gated_backend import GatedBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


def test_gated_backend_importable():
    assert GatedBackend is not None


def test_gated_backend_is_base_subclass():
    assert issubclass(GatedBackend, BaseBackend)


def test_gated_backend_instantiable():
    inner = MemoryBackend()
    b = GatedBackend(inner, gate_fn=lambda: True, reason="test")
    assert isinstance(b, GatedBackend)


def test_gated_backend_has_expected_attrs():
    inner = MemoryBackend()
    b = GatedBackend(inner, gate_fn=lambda: True, reason="test")
    assert hasattr(b, "inner")
    assert hasattr(b, "reason")
    assert hasattr(b, "is_open")
    assert hasattr(b, "acquire")
    assert hasattr(b, "release")
    assert hasattr(b, "is_locked")
    assert hasattr(b, "refresh")
