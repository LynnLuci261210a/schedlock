"""Verify ShieldBackend is importable from the backends package."""
import pytest

from schedlock.backends.shield_backend import ShieldBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


def test_shield_backend_importable():
    assert ShieldBackend is not None


def test_shield_backend_is_base_subclass():
    assert issubclass(ShieldBackend, BaseBackend)


def test_shield_backend_instantiable():
    inner = MemoryBackend()
    backend = ShieldBackend(inner)
    assert isinstance(backend, ShieldBackend)


def test_shield_backend_has_expected_attrs():
    inner = MemoryBackend()
    backend = ShieldBackend(inner, reason="deploy freeze")
    assert hasattr(backend, "inner")
    assert hasattr(backend, "reason")
    assert hasattr(backend, "is_shielded")
    assert hasattr(backend, "raise_shield")
    assert hasattr(backend, "lower_shield")
    assert backend.reason == "deploy freeze"
