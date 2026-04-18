"""RotatingBackend — cycles through a list of inner backends on each acquire.

Useful for spreading lock acquisition load across multiple backends in a
round-robin fashion.
"""

from __future__ import annotations

import threading
from typing import List, Optional

from schedlock.backends.base import BaseBackend


class RotatingBackend(BaseBackend):
    """Delegates acquire to backends in round-robin order.

    release and is_locked are always forwarded to the backend that
    currently holds the lock for the given key (tracked internally).
    If no record exists for a key, all backends are tried in order.
    """

    def __init__(self, backends: List[BaseBackend]) -> None:
        if not backends:
            raise ValueError("RotatingBackend requires at least one backend.")
        self._backends = list(backends)
        self._index = 0
        self._lock = threading.Lock()
        # key -> backend index that last successfully acquired
        self._owner_map: dict[str, int] = {}

    @property
    def backends(self) -> List[BaseBackend]:
        return list(self._backends)

    def _next_index(self) -> int:
        with self._lock:
            idx = self._index
            self._index = (self._index + 1) % len(self._backends)
            return idx

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        start = self._next_index()
        for offset in range(len(self._backends)):
            idx = (start + offset) % len(self._backends)
            backend = self._backends[idx]
            if backend.acquire(key, owner, ttl):
                with self._lock:
                    self._owner_map[key] = idx
                return True
        return False

    def release(self, key: str, owner: str) -> bool:
        with self._lock:
            idx = self._owner_map.get(key)
        if idx is not None:
            result = self._backends[idx].release(key, owner)
            if result:
                with self._lock:
                    self._owner_map.pop(key, None)
            return result
        # Fallback: try all
        for backend in self._backends:
            if backend.release(key, owner):
                return True
        return False

    def is_locked(self, key: str) -> bool:
        return any(b.is_locked(key) for b in self._backends)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        with self._lock:
            idx = self._owner_map.get(key)
        if idx is not None:
            return self._backends[idx].refresh(key, owner, ttl)
        return False
