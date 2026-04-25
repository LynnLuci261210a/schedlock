"""Verify BouncerBackend is correctly exposed via the backends package."""
import pytest

from schedlock.backends.bouncer_backend import BouncerBackend
from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend


def test_bouncer_backend_importable():
    assert BouncerBackend is not None


def test_bouncer_backend_is_base_subclass():
    assert issubclass(BouncerBackend, BaseBackend)


def test_bouncer_backend_instantiable():
    inner = MemoryBackend()
    b = BouncerBackend(inner, bouncer=lambda k, o: True)
    assert isinstance(b, BouncerBackend)


def test_bouncer_backend_has_expected_attrs():
    inner = MemoryBackend()
    b = BouncerBackend(inner, bouncer=lambda k, o: True, reason="test")
    assert hasattr(b, "inner")
    assert hasattr(b, "reason")
    assert hasattr(b, "acquire")
    assert hasattr(b, "release")
    assert hasattr(b, "is_locked")
    assert hasattr(b, "refresh")
