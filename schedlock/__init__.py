"""schedlock — lightweight distributed cron-style job locking.

Public API
----------
schedlock
    Decorator that prevents overlapping execution of scheduled jobs.
FileBackend
    File-system based locking backend (suitable for single-host deployments).
RedisBackend
    Redis-based locking backend (suitable for distributed deployments).
"""

from schedlock.backends.file_backend import FileBackend
from schedlock.backends.redis_backend import RedisBackend
from schedlock.decorator import schedlock

__all__ = [
    "schedlock",
    "FileBackend",
    "RedisBackend",
]

__version__ = "0.1.0"
