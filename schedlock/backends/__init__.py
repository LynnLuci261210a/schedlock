"""Backend registry for schedlock."""

from schedlock.backends.base import BaseBackend
from schedlock.backends.file_backend import FileBackend
from schedlock.backends.memory_backend import MemoryBackend
from schedlock.backends.composite import CompositeBackend

try:
    from schedlock.backends.redis_backend import RedisBackend
except ImportError:  # pragma: no cover
    RedisBackend = None  # type: ignore

__all__ = [
    "BaseBackend",
    "FileBackend",
    "MemoryBackend",
    "CompositeBackend",
    "RedisBackend",
]
