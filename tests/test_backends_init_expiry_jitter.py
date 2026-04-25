"""Tests that ExpiryJitterBackend is correctly exposed via the backends package."""

import pytest

from schedlock.backends.expiry_jitter_backend import ExpiryJitterBackend
from schedlock.backends.base import BaseBackend


def test_expiry_jitter_backend_importable():
    assert ExpiryJitterBackend is not None


def test_expiry_jitter_backend_is_base_subclass():
    assert issubclass(ExpiryJitterBackend, BaseBackend)


def test_expiry_jitter_backend_instantiable():
    from schedlock.backends.memory_backend import MemoryBackend
    inner = MemoryBackend()
    b = ExpiryJitterBackend(inner, max_jitter=1.0)
    assert isinstance(b, ExpiryJitterBackend)
    assert isinstance(b, BaseBackend)


def test_expiry_jitter_backend_has_expected_attrs():
    from schedlock.backends.memory_backend import MemoryBackend
    inner = MemoryBackend()
    b = ExpiryJitterBackend(inner, max_jitter=2.5, seed=0)
    assert hasattr(b, "inner")
    assert hasattr(b, "max_jitter")
    assert b.max_jitter == 2.5
    assert b.inner is inner
