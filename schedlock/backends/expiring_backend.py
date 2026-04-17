"""ExpiringBackend — wraps any backend and auto-releases locks past a hard deadline."""

from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class ExpiringBackend(BaseBackend):
    """Wraps an inner backend and enforces a hard expiry wall-clock deadline.

    If the lock was acquired more than *max_age* seconds ago (tracked locally),
    any subsequent call to ``is_locked`` or ``acquire`` will treat the lock as
    expired and attempt to release it before proceeding.
    """

    def __init__(self, inner: BaseBackend, max_age: float = 3600.0) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if max_age <= 0:
            raise ValueError("max_age must be a positive number")
        self._inner = inner
        self._max_age = max_age
        # key -> (owner, acquired_at)
        self._registry: dict[str, tuple[str, float]] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def _evict_if_expired(self, key: str) -> None:
        entry = self._registry.get(key)
        if entry and (time.monotonic() - entry[1]) > self._max_age:
            owner, _ = entry
            self._inner.release(key, owner)
            del self._registry[key]

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        self._evict_if_expired(key)
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            self._registry[key] = (owner, time.monotonic())
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._registry.pop(key, None)
        return released

    def is_locked(self, key: str) -> bool:
        self._evict_if_expired(key)
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
