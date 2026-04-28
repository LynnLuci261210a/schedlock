from __future__ import annotations

from typing import Optional, Set

from schedlock.backends.base import BaseBackend


class DenylistBackend(BaseBackend):
    """
    A backend wrapper that blocks acquire/release operations for owners
    whose identifiers appear in a denylist.

    Unlike BlacklistBackend (which is key-agnostic), DenylistBackend
    supports per-key deny rules via a ``{key: {owner, ...}}`` mapping
    as well as a global set of always-denied owners.
    """

    def __init__(
        self,
        inner: BaseBackend,
        *,
        global_denied: Optional[Set[str]] = None,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._global_denied: Set[str] = set(global_denied or [])
        self._per_key_denied: dict[str, Set[str]] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def deny(self, owner: str, *, key: Optional[str] = None) -> None:
        """Add *owner* to the denylist, optionally scoped to *key*."""
        if not owner or not isinstance(owner, str):
            raise ValueError("owner must be a non-empty string")
        if key is None:
            self._global_denied.add(owner)
        else:
            self._per_key_denied.setdefault(key, set()).add(owner)

    def allow(self, owner: str, *, key: Optional[str] = None) -> None:
        """Remove *owner* from the denylist (globally or for *key*)."""
        if key is None:
            self._global_denied.discard(owner)
        else:
            if key in self._per_key_denied:
                self._per_key_denied[key].discard(owner)

    def _is_denied(self, key: str, owner: str) -> bool:
        if owner in self._global_denied:
            return True
        return owner in self._per_key_denied.get(key, set())

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._is_denied(key, owner):
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        if self._is_denied(key, owner):
            return False
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        if self._is_denied(key, owner):
            return False
        return self._inner.refresh(key, owner, ttl)
