"""StickyOwnerBackend — once a key is acquired, only that owner may re-acquire
until the lock is explicitly released (even across TTL expiry in the inner
backend).  This is distinct from StickyBackend in that the binding is
persisted independently of the inner lock state."""

from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class StickyOwnerBackend(BaseBackend):
    """Wraps an inner backend and enforces single-owner stickiness per key.

    Once *owner* A acquires *key*, no other owner may acquire *key* until
    owner A releases it — even if the underlying TTL has expired.

    Args:
        inner: The backend to delegate real locking to.
    """

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._bound: dict[str, str] = {}  # key -> owner

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        bound_owner = self._bound.get(key)
        if bound_owner is not None and bound_owner != owner:
            return False
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            self._bound[key] = owner
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released and self._bound.get(key) == owner:
            del self._bound[key]
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def bound_owner(self, key: str) -> Optional[str]:
        """Return the currently bound owner for *key*, or None."""
        return self._bound.get(key)
