from __future__ import annotations
from typing import Optional
from schedlock.backends.base import BaseBackend


class WatermarkBackend(BaseBackend):
    """Tracks high-water mark (peak concurrent holders) across acquire/release calls."""

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._active: dict[str, set[str]] = {}
        self._peak: dict[str, int] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def peak_for(self, key: str) -> int:
        """Return the peak concurrent holder count observed for a key."""
        return self._peak.get(key, 0)

    def current_for(self, key: str) -> int:
        """Return the current active holder count for a key."""
        return len(self._active.get(key, set()))

    def reset_peak(self, key: str) -> None:
        """Reset the peak counter for a key."""
        self._peak[key] = len(self._active.get(key, set()))

    def acquire(self, key: str, owner: str, ttl: int = 60) -> bool:
        result = self._inner.acquire(key, owner, ttl)
        if result:
            holders = self._active.setdefault(key, set())
            holders.add(owner)
            current = len(holders)
            if current > self._peak.get(key, 0):
                self._peak[key] = current
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        if result:
            holders = self._active.get(key, set())
            holders.discard(owner)
        return result

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int = 60) -> bool:
        return self._inner.refresh(key, owner, ttl)
