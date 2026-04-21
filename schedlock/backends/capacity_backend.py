from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class CapacityBackend(BaseBackend):
    """
    A backend wrapper that tracks and enforces a maximum number of
    concurrent lock holders across all keys combined.

    Unlike CappedBackend (which caps per-key), CapacityBackend enforces
    a global ceiling across every key managed by this instance.
    """

    def __init__(self, inner: BaseBackend, max_capacity: int) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_capacity, int) or max_capacity < 1:
            raise ValueError("max_capacity must be a positive integer")
        self._inner = inner
        self._max_capacity = max_capacity
        # Maps (key, owner) -> True for currently held locks
        self._holders: dict[tuple[str, str], bool] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_capacity(self) -> int:
        return self._max_capacity

    @property
    def current_load(self) -> int:
        """Return the number of currently held locks across all keys."""
        return len(self._holders)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self.current_load >= self._max_capacity:
            return False
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            self._holders[(key, owner)] = True
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._holders.pop((key, owner), None)
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
