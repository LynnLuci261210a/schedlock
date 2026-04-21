"""RateWindowBackend: limits acquires to N per rolling time window per key."""

import time
from collections import defaultdict
from typing import Optional

from schedlock.backends.base import BaseBackend


class RateWindowBackend(BaseBackend):
    """Wraps a backend and enforces a maximum number of acquires per rolling window.

    Unlike ThrottledBackend (which is global), this tracks acquire counts
    independently per lock key, making it suitable for multi-key workloads.
    """

    def __init__(self, inner: BaseBackend, max_acquires: int, window: float) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if max_acquires < 1:
            raise ValueError("max_acquires must be a positive integer")
        if window <= 0:
            raise ValueError("window must be a positive number")
        self._inner = inner
        self._max_acquires = max_acquires
        self._window = window
        # key -> list of acquire timestamps
        self._timestamps: dict[str, list[float]] = defaultdict(list)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_acquires(self) -> int:
        return self._max_acquires

    @property
    def window(self) -> float:
        return self._window

    def _prune(self, key: str) -> None:
        cutoff = time.monotonic() - self._window
        self._timestamps[key] = [
            ts for ts in self._timestamps[key] if ts > cutoff
        ]

    def _at_limit(self, key: str) -> bool:
        self._prune(key)
        return len(self._timestamps[key]) >= self._max_acquires

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._at_limit(key):
            return False
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._timestamps[key].append(time.monotonic())
        return result

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def count_for(self, key: str) -> int:
        """Return the number of acquires recorded in the current window for key."""
        self._prune(key)
        return len(self._timestamps[key])
