"""Verify CircuitRetryBackend is properly exported from the backends package."""
from __future__ import annotations

import pytest


def test_circuit_retry_backend_importable():
    from schedlock.backends.circuit_retry_backend import CircuitRetryBackend
    assert CircuitRetryBackend is not None


def test_circuit_retry_backend_is_base_subclass():
    from schedlock.backends.circuit_retry_backend import CircuitRetryBackend
    from schedlock.backends.base import BaseBackend
    assert issubclass(CircuitRetryBackend, BaseBackend)


def test_circuit_retry_backend_has_expected_attrs():
    from schedlock.backends.circuit_retry_backend import CircuitRetryBackend
    from schedlock.backends.memory_backend import MemoryBackend
    b = CircuitRetryBackend(
        MemoryBackend(),
        retries=2,
        delay=0.0,
        failure_threshold=3,
        reset_timeout=10.0,
    )
    assert hasattr(b, "inner")
    assert hasattr(b, "is_open")
    assert hasattr(b, "acquire")
    assert hasattr(b, "release")
    assert hasattr(b, "is_locked")
    assert hasattr(b, "refresh")
