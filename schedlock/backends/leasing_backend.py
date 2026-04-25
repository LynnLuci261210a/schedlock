from __future__ import annotations

import time
from typing import Dict, Optional, Tuple

from schedlock.backends.base import BaseBackend


class LeasingBackend(BaseBackend):
    """Wraps a backend and enforces renewable leases with a fixed lease duration.

    Unlike a plain TTL, a lease can be explicitly renewed by the current owner.
    Attempts to acquire an already-leased key by a *different* owner are blocked
    until the lease expires naturally or is released.
    """

    def __init__(
        self,
        inner: BaseBackend,
        lease_seconds: float = 30.0,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(lease_seconds, (int, float)) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive number")
        self._inner = inner
        self._lease_seconds = float(lease_seconds)
        # key -> (owner, expiry_timestamp)
        self._leases: Dict[str, Tuple[str, float]] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def lease_seconds(self) -> float:
        return self._lease_seconds

    def _evict_if_expired(self, key: str) -> None:
        entry = self._leases.get(key)
        if entry and time.monotonic() >= entry[1]:
            del self._leases[key]

    def acquire(self, key: str, owner: str, ttl: int = 30) -> bool:
        self._evict_if_expired(key)
        entry = self._leases.get(key)
        if entry is not None and entry[0] != owner:
            return False
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            self._leases[key] = (owner, time.monotonic() + self._lease_seconds)
        return acquired

    def release(self, key: str, owner: str) -> bool:
        self._evict_if_expired(key)
        entry = self._leases.get(key)
        if entry is not None and entry[0] != owner:
            return False
        released = self._inner.release(key, owner)
        if released:
            self._leases.pop(key, None)
        return released

    def is_locked(self, key: str) -> bool:
        self._evict_if_expired(key)
        if key not in self._leases:
            return False
        return self._inner.is_locked(key)

    def renew(self, key: str, owner: str) -> bool:
        """Renew the lease for *owner* on *key*. Returns True if renewed."""
        self._evict_if_expired(key)
        entry = self._leases.get(key)
        if entry is None or entry[0] != owner:
            return False
        self._leases[key] = (owner, time.monotonic() + self._lease_seconds)
        return True

    def lease_expires_at(self, key: str) -> Optional[float]:
        """Return the monotonic expiry timestamp for *key*, or None."""
        self._evict_if_expired(key)
        entry = self._leases.get(key)
        return entry[1] if entry else None
