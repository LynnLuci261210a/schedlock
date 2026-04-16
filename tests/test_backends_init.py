"""Tests for schedlock.backends package exports."""

from schedlock.backends import (
    BaseBackend,
    FileBackend,
    MemoryBackend,
    CompositeBackend,
    AuditedBackend,
    QuotaBackend,
    RateLimitedBackend,
    ReadOnlyBackend,
    TTLCapBackend,
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


def test_composite_backend_importable():
    assert CompositeBackend is not None


def test_audited_backend_importable():
    assert AuditedBackend is not None


def test_quota_backend_importable():
    assert QuotaBackend is not None


def test_ratelimited_backend_importable():
    assert RateLimitedBackend is not None


def test_readonly_backend_importable():
    assert ReadOnlyBackend is not None


def test_ttl_cap_backend_importable():
    assert TTLCapBackend is not None


def test_ttl_cap_backend_is_base_subclass():
    assert issubclass(TTLCapBackend, BaseBackend)
