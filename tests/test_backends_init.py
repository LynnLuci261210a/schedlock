import pytest
from schedlock.backends import (
    BaseBackend,
    MemoryBackend,
    FileBackend,
    RedisBackend,
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
    PrefixedBackend,
    EncryptedBackend,
    SnapshotBackend,
    ThrottledBackend,
    PrioritizedBackend,
    ConditionalBackend,
    ObservableBackend,
    DebounceBackend,
    ShadowBackend,
    TimedBackend,
    SamplingBackend,
    ValidatingBackend,
    CoalescingBackend,
    VersionedBackend,
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


def test_versioned_backend_importable():
    assert VersionedBackend is not None


def test_versioned_backend_is_base_subclass():
    assert issubclass(VersionedBackend, BaseBackend)


def test_all_wrappers_are_base_subclasses():
    wrappers = [
        CompositeBackend, AuditedBackend, QuotaBackend, RateLimitedBackend,
        ReadOnlyBackend, TTLCapBackend, TaggedBackend, NamespacedBackend,
        FallbackBackend, RetryBackend, CachedBackend, MetricsBackend,
        LoggingBackend, CircuitBreakerBackend, ExpiringBackend, PrefixedBackend,
        EncryptedBackend, SnapshotBackend, ThrottledBackend, PrioritizedBackend,
        ConditionalBackend, ObservableBackend, DebounceBackend, ShadowBackend,
        TimedBackend, SamplingBackend, ValidatingBackend, CoalescingBackend,
        VersionedBackend,
    ]
    for cls in wrappers:
        assert issubclass(cls, BaseBackend), f"{cls.__name__} not a BaseBackend subclass"
