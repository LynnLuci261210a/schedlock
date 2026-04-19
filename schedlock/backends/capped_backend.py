"""CappedBackend — limits the number of concurrently held locks."""
from __future__ import annotations

import threading
from typing import Optional

from schedlock.backends.base import BaseBackend


class CappedBackend(BaseBackend):
    """Wraps a backend and enforces a maximum number of concurrent lock holders."""

    def __init__(self, inner: BaseBackend, max_holders: int = 1) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_holders, int) or max_holders < 1:
            raise ValueError("max_holders must be a positive integer")
        self._inner = inner
        self._max_holders = max_holders
        self._held: dict[str, set[str]] = {}  # key -> set of owner ids
        self._lock = threading.Lock()

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_holders(self) -> int:
        return self._max_holders

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        with self._lock:
            holders = self._held.get(key, set())
            if owner in holders:
                return True
            if len(holders) >= self._max_holders:
                return False
            acquired = self._inner.acquire(key, owner, ttl)
            if acquired:
                self._held.setdefault(key, set()).add(owner)
            return acquired

    def release(self, key: str, owner: str) -> bool:
        with self._lock:
            released = self._inner.release(key, owner)
            if released:
                self._held.get(key, set()).discard(owner)
            return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
