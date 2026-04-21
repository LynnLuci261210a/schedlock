"""QuotaAwareBackend — enforces per-owner acquire quotas with automatic window reset."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from schedlock.backends.base import BaseBackend


class QuotaAwareBackend(BaseBackend):
    """Wraps a backend and enforces per-owner acquire quotas within a rolling window.

    Each owner may acquire at most *max_per_owner* times within *window* seconds.
    Releases do not restore quota — the window rolls naturally.
    """

    def __init__(
        self,
        inner: BaseBackend,
        *,
        max_per_owner: int = 5,
        window: float = 60.0,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if max_per_owner < 1:
            raise ValueError("max_per_owner must be a positive integer")
        if window <= 0:
            raise ValueError("window must be a positive number")

        self._inner = inner
        self._max_per_owner = max_per_owner
        self._window = window
        # owner -> list of acquire timestamps within current window
        self._history: Dict[str, List[float]] = defaultdict(list)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_per_owner(self) -> int:
        return self._max_per_owner

    @property
    def window(self) -> float:
        return self._window

    def _prune(self, owner: str, now: float) -> None:
        cutoff = now - self._window
        self._history[owner] = [
            ts for ts in self._history[owner] if ts > cutoff
        ]

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        now = time.monotonic()
        self._prune(owner, now)
        if len(self._history[owner]) >= self._max_per_owner:
            return False
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._history[owner].append(now)
        return result

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def quota_used(self, owner: str) -> int:
        """Return how many acquires the owner has used in the current window."""
        now = time.monotonic()
        self._prune(owner, now)
        return len(self._history[owner])

    def quota_remaining(self, owner: str) -> int:
        """Return how many more acquires the owner may perform in the current window."""
        return max(0, self._max_per_owner - self.quota_used(owner))
