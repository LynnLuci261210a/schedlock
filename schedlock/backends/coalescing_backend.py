"""CoalescingBackend: deduplicates rapid acquire attempts for the same key.

If a successful acquire was made for a given key within `window` seconds,
subsequent acquire calls return False without hitting the inner backend.
This prevents thundering-herd bursts from hammering the underlying store.
"""

from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class CoalescingBackend(BaseBackend):
    """Wraps an inner backend and coalesces repeated acquire attempts."""

    def __init__(self, inner: BaseBackend, window: float = 1.0) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(window, (int, float)) or window <= 0:
            raise ValueError("window must be a positive number")
        self._inner = inner
        self._window = float(window)
        self._last_acquired: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def window(self) -> float:
        return self._window

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        now = time.monotonic()
        last = self._last_acquired.get(key)
        if last is not None and (now - last) < self._window:
            return False
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._last_acquired[key] = now
        return result

    def release(self, key: str, owner: str) -> bool:
        self._last_acquired.pop(key, None)
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
