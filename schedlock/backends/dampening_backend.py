"""DampeningBackend — suppresses repeated acquire attempts for the same key
within a configurable cool-off period after a *failed* acquire.

If a caller tries to acquire a lock and the inner backend returns False,
the key is placed in a "dampened" state for `dampening_seconds`.  Any
further acquire attempts on that key during that window are rejected
immediately without consulting the inner backend, reducing thundering-herd
pressure on the underlying store.
"""
from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class DampeningBackend(BaseBackend):
    """Suppress repeated acquire attempts after a failed acquire."""

    def __init__(
        self,
        inner: BaseBackend,
        dampening_seconds: float = 5.0,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(dampening_seconds, (int, float)) or dampening_seconds <= 0:
            raise ValueError("dampening_seconds must be a positive number")

        self._inner = inner
        self._dampening_seconds = float(dampening_seconds)
        # Maps lock_key -> timestamp when dampening expires
        self._dampened: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def dampening_seconds(self) -> float:
        return self._dampening_seconds

    def _is_dampened(self, key: str) -> bool:
        expiry = self._dampened.get(key)
        if expiry is None:
            return False
        if time.monotonic() < expiry:
            return True
        del self._dampened[key]
        return False

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._is_dampened(key):
            return False
        result = self._inner.acquire(key, owner, ttl)
        if not result:
            self._dampened[key] = time.monotonic() + self._dampening_seconds
        return result

    def release(self, key: str, owner: str) -> bool:
        self._dampened.pop(key, None)
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
