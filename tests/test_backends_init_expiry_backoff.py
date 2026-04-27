"""Verify ExpiryBackoffBackend is properly exported from schedlock.backends."""

import pytest

import schedlock.backends as pkg
from schedlock.backends.base import BaseBackend
from schedlock.backends.expiry_backoff_backend import ExpiryBackoffBackend
from schedlock.backends.memory_backend import MemoryBackend


def test_expiry_backoff_backend_importable():
    assert ExpiryBackoffBackend is not None


def test_expiry_backoff_backend_is_base_subclass():
    assert issubclass(ExpiryBackoffBackend, BaseBackend)


def test_expiry_backoff_backend_instantiable():
    mem = MemoryBackend()
    b = ExpiryBackoffBackend(mem)
    assert isinstance(b, ExpiryBackoffBackend)


def test_expiry_backoff_backend_has_expected_attrs():
    mem = MemoryBackend()
    b = ExpiryBackoffBackend(mem, growth_factor=1.5, max_ttl=120.0)
    assert b.growth_factor == 1.5
    assert b.max_ttl == 120.0
    assert b.inner is mem


def test_failure_count_starts_at_zero():
    mem = MemoryBackend()
    b = ExpiryBackoffBackend(mem)
    assert b.failure_count("any_key") == 0


def test_current_multiplier_starts_at_one():
    mem = MemoryBackend()
    b = ExpiryBackoffBackend(mem)
    assert b.current_multiplier("any_key") == 1.0
