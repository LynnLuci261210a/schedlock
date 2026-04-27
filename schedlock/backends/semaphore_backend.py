"""SemaphoreBackend — limits concurrent lock holders to a fixed count per key."""
from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class SemaphoreBackend(BaseBackend):
    """Wraps an inner backend and enforces a maximum number of concurrent
    holders for each lock key.  Internally the active holder set is tracked
    in memory; the inner backend still arbitrates the actual lock record.
    """

    def __init__(self, inner: BaseBackend, *, max_holders: int = 1) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_holders, int) or max_holders < 1:
            raise ValueError("max_holders must be a positive integer")
        self._inner = inner
        self._max_holders = max_holders
        # key -> set of owner strings currently holding the lock
        self._holders: dict[str, set[str]] = {}

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_holders(self) -> int:
        return self._max_holders

    def slots_available(self, key: str) -> int:
        """Return how many slots are still open for *key*."""
        return self._max_holders - len(self._holders.get(key, set()))

    def current_holders(self, key: str) -> set[str]:
        """Return a copy of the set of owners currently holding *key*."""
        return set(self._holders.get(key, set()))

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        holders = self._holders.setdefault(key, set())
        if owner in holders:
            # Re-entrant: already counted, just refresh the inner lock.
            return self._inner.acquire(key, owner, ttl)
        if len(holders) >= self._max_holders:
            return False
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            holders.add(owner)
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._holders.get(key, set()).discard(owner)
        return released

    def is_locked(self, key: str) -> bool:
        return bool(self._holders.get(key))

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
