"""PinnedBackend — only allows a fixed set of owners to acquire the lock."""

from __future__ import annotations

from typing import Collection, Optional

from schedlock.backends.base import BaseBackend


class PinnedBackend(BaseBackend):
    """Wraps a backend and restricts lock acquisition to a pinned set of owners.

    Args:
        inner: The backend to delegate to.
        allowed_owners: A collection of owner strings permitted to acquire.
    """

    def __init__(self, inner: BaseBackend, allowed_owners: Collection[str]) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        allowed = list(allowed_owners)
        if not allowed:
            raise ValueError("allowed_owners must not be empty")
        self._inner = inner
        self._allowed: frozenset[str] = frozenset(allowed)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def allowed_owners(self) -> frozenset[str]:
        return self._allowed

    def _check_owner(self, owner: str) -> None:
        """Raise ValueError if owner is not in the allowed set."""
        if owner not in self._allowed:
            raise ValueError(
                f"Owner {owner!r} is not in the allowed set: {self._allowed}"
            )

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if owner not in self._allowed:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        if owner not in self._allowed:
            return False
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        if owner not in self._allowed:
            return False
        return self._inner.refresh(key, owner, ttl)

    def get_current_owner(self, key: str) -> Optional[str]:
        """Return the current owner of the lock, or None if not locked.

        Delegates to the inner backend's get_current_owner if available.

        Returns:
            The owner string if the key is locked, otherwise None.
        """
        return self._inner.get_current_owner(key)
