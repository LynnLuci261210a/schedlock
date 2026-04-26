from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class StickyKeyBackend(BaseBackend):
    """
    A backend wrapper that, once a key is successfully acquired by an owner,
    remembers that owner and only allows *that* owner to re-acquire the same
    key until the lock is explicitly released.  After release the binding is
    cleared and any owner may acquire the key again.

    This is useful when you want to guarantee that a recurring job always
    runs on the same worker node for the lifetime of a logical task, even
    if the underlying TTL expires between runs.
    """

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        # key -> owner string
        self._bound: dict[str, str] = {}

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
        if released:
            self._bound.pop(key, None)
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def bound_owner(self, key: str) -> Optional[str]:
        """Return the currently bound owner for *key*, or None."""
        return self._bound.get(key)

    def clear_binding(self, key: str) -> None:
        """Forcefully remove the sticky binding for *key* without releasing
        the underlying lock.  Use with care."""
        self._bound.pop(key, None)
