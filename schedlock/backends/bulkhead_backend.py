"""BulkheadBackend — limits concurrent lock holders across a shared partition."""
from __future__ import annotations

import threading
from typing import Optional

from schedlock.backends.base import BaseBackend


class BulkheadBackend(BaseBackend):
    """Wraps a backend and enforces a maximum number of concurrent active locks
    across all keys within this bulkhead partition.

    Unlike CappedBackend (per-key cap), BulkheadBackend caps the *total* number
    of simultaneously held locks regardless of key.
    """

    def __init__(self, inner: BaseBackend, *, max_concurrent: int = 1) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_concurrent, int) or max_concurrent < 1:
            raise ValueError("max_concurrent must be a positive integer")
        self._inner = inner
        self._max_concurrent = max_concurrent
        self._active: dict[str, str] = {}  # key -> owner
        self._lock = threading.Lock()

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_concurrent(self) -> int:
        return self._max_concurrent

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        with self._lock:
            if len(self._active) >= self._max_concurrent and key not in self._active:
                return False
            acquired = self._inner.acquire(key, owner, ttl)
            if acquired:
                self._active[key] = owner
            return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            with self._lock:
                if self._active.get(key) == owner:
                    del self._active[key]
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def active_count(self) -> int:
        """Return the number of currently tracked active locks."""
        with self._lock:
            return len(self._active)
