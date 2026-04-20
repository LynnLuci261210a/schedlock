"""TieredBackend: tries backends in priority order, using the first that succeeds."""

from __future__ import annotations

from typing import List, Optional

from schedlock.backends.base import BaseBackend


class TieredBackend(BaseBackend):
    """Attempts acquire/release across a prioritized list of backends.

    Backends are tried in order (index 0 = highest priority).  The first
    backend that successfully acquires the lock is used; subsequent backends
    are not contacted.  On release, the call is forwarded to every backend
    so that any state written during acquire is cleaned up.
    """

    def __init__(self, backends: List[BaseBackend]) -> None:
        if not backends:
            raise ValueError("TieredBackend requires at least one backend.")
        self._backends = list(backends)

    @property
    def backends(self) -> List[BaseBackend]:
        """Return the ordered list of backends."""
        return list(self._backends)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        """Try each backend in priority order; return True on first success."""
        for backend in self._backends:
            if backend.acquire(key, owner, ttl):
                return True
        return False

    def release(self, key: str, owner: str) -> bool:
        """Forward release to all backends; return True if any succeeded."""
        released = False
        for backend in self._backends:
            if backend.release(key, owner):
                released = True
        return released

    def is_locked(self, key: str) -> bool:
        """Return True if any backend reports the key as locked."""
        return any(b.is_locked(key) for b in self._backends)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        """Refresh TTL on all backends; return True if any succeeded."""
        refreshed = False
        for backend in self._backends:
            if backend.refresh(key, owner, ttl):
                refreshed = True
        return refreshed
