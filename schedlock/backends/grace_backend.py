"""GraceBackend: allows a configurable grace period after a lock expires before
allowing re-acquisition. Useful for preventing thundering-herd scenarios where
multiple workers race to acquire a just-released lock."""

from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class GraceBackend(BaseBackend):
    """Wraps an inner backend and enforces a grace period after release.

    After a lock is released (or expires), no owner may re-acquire it until
    ``grace_seconds`` have elapsed.
    """

    def __init__(self, inner: BaseBackend, grace_seconds: float) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(grace_seconds, (int, float)) or grace_seconds <= 0:
            raise ValueError("grace_seconds must be a positive number")
        self._inner = inner
        self._grace_seconds = grace_seconds
        # Maps lock_key -> timestamp when grace period ends
        self._grace_until: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def grace_seconds(self) -> float:
        return self._grace_seconds

    def _in_grace(self, key: str) -> bool:
        until = self._grace_until.get(key)
        if until is None:
            return False
        if time.monotonic() < until:
            return True
        del self._grace_until[key]
        return False

    def acquire(self, key: str, owner: str, ttl: int = 60) -> bool:
        if self._in_grace(key):
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._grace_until[key] = time.monotonic() + self._grace_seconds
        return released

    def is_locked(self, key: str) -> bool:
        if self._in_grace(key):
            return True
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int = 60) -> bool:
        return self._inner.refresh(key, owner, ttl)
