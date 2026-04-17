"""Backend registry for schedlock."""

from schedlock.backends.base import BaseBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.file_backend import FileBackend
from schedlock.backends.composite import CompositeBackend
from schedlock.backends.audited_backend import AuditedBackend
from schedlock.backends.quota_backend import QuotaBackend
from schedlock.backends.ratelimited_backend import RateLimitedBackend
from schedlock.backends.readonly_backend import ReadOnlyBackend
from schedlock.backends.ttl_backend import TTLCapBackend
from schedlock.backends.tagged_backend import TaggedBackend
from schedlock.backends.namespaced_backend import NamespacedBackend
from schedlock.backends.fallback_backend import FallbackBackend
from schedlock.backends.retry_backend import RetryBackend
from schedlock.backends.cached_backend import CachedBackend
from schedlock.backends.metrics_backend import MetricsBackend
from schedlock.backends.logging_backend import LoggingBackend
from schedlock.backends.circuit_breaker_backend import CircuitBreakerBackend

try:
    from schedlock.backends.redis_backend import RedisBackend
except ImportError:  # pragma: no cover
    RedisBackend = None  # type: ignore

__all__ = [
    "BaseBackend",
    "MemoryBackend",
    "FileBackend",
    "CompositeBackend",
    "AuditedBackend",
    "QuotaBackend",
    "RateLimitedBackend",
    "ReadOnlyBackend",
    "TTLCapBackend",
    "TaggedBackend",
    "NamespacedBackend",
    "FallbackBackend",
    "RetryBackend",
    "CachedBackend",
    "MetricsBackend",
    "LoggingBackend",
    "CircuitBreakerBackend",
    "RedisBackend",
]
