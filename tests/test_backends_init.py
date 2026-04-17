"""Tests for schedlock.backends package imports."""

import pytest

from schedlock.backends import (
    BaseBackend,
    MemoryBackend,
    FileBackend,
    CompositeBackend,
    AuditedBackend,
    QuotaBackend,
    RateLimitedBackend,
    ReadOnlyBackend,
    TTLCapBackend,
    TaggedBackend,
    NamespacedBackend,
    FallbackBackend,
    RetryBackend,
    CachedBackend,
    MetricsBackend,
    LoggingBackend,
    CircuitBreakerBackend,
    ExpiringBackend,
)


def test_base_backend_importable():
    assert BaseBackend is not None


def test_file_backend_importable():
    assert FileBackend is not None


def test_memory_backend_importable():
    assert MemoryBackend is not None


def test_memory_backend_is_base_subclass():
    assert issubclass(MemoryBackend, BaseBackend)


def test_file_backend_is_base_subclass():
    assert issubclass(FileBackend, BaseBackend)


def test_expiring_backend_importable():
    assert ExpiringBackend is not None


def test_expiring_backend_is_base_subclass():
    assert issubclass(ExpiringBackend, BaseBackend)


def test_composite_backend_importable():
    assert CompositeBackend is not None


def test_metrics_backend_importable():
    assert MetricsBackend is not None


def test_circuit_breaker_backend_importable():
    assert CircuitBreakerBackend is not None


def test_all_wrappers_are_base_subclasses():
    wrappers = [
        CompositeBackend, AuditedBackend, QuotaBackend, RateLimitedBackend,
        ReadOnlyBackend, TTLCapBackend, TaggedBackend, NamespacedBackend,
        FallbackBackend, RetryBackend, CachedBackend, MetricsBackend,
        LoggingBackend, CircuitBreakerBackend, ExpiringBackend,
    ]
    for cls in wrappers:
        assert issubclass(cls, BaseBackend), f"{cls.__name__} not a BaseBackend subclass"
