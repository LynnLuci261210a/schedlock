"""StickyBackend — once a key is acquired by an owner, only that owner
can re-acquire it until the lock is explicitly released."""
from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class StickyBackend(BaseBackend):
    """Wraps an inner backend and remembers the last successful owner per key.
    Subsequent acquire attempts by a *different* owner are rejected even if
    the inner lock has expired and is technically free.
    Call release() with the original owner to clear the sticky binding.
    """

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._sticky: dict[str, str] = {}  # key -> owner

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        bound = self._sticky.get(key)
        if bound is not None and bound != owner:
            return False
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            self._sticky[key] = owner
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released and self._sticky.get(key) == owner:
            del self._sticky[key]
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def sticky_owner(self, key: str) -> Optional[str]:
        """Return the currently bound sticky owner for *key*, or None."""
        return self._sticky.get(key)
