"""CeilingBackend — caps the number of successful acquires per key over all time.

Unlike BudgetBackend (which counts globally), CeilingBackend tracks per-key
acquire counts independently and refuses further acquires once the ceiling
is reached for that key, regardless of releases.
"""

from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class CeilingBackend(BaseBackend):
    """Wraps an inner backend and enforces a lifetime acquire ceiling per key.

    Once a key has been acquired ``max_acquires`` times in total, all further
    acquire attempts for that key are rejected permanently (until the backend
    instance is replaced or the counter is reset via :meth:`reset`).

    Args:
        inner: The underlying backend to delegate to.
        max_acquires: Maximum number of lifetime acquires allowed per key.
            Must be a positive integer.
    """

    def __init__(self, inner: BaseBackend, *, max_acquires: int = 1) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_acquires, int) or max_acquires < 1:
            raise ValueError("max_acquires must be a positive integer")
        self._inner = inner
        self._max_acquires = max_acquires
        self._counts: dict[str, int] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_acquires(self) -> int:
        return self._max_acquires

    def count_for(self, key: str) -> int:
        """Return the number of lifetime successful acquires for *key*."""
        return self._counts.get(key, 0)

    def reset(self, key: str) -> None:
        """Reset the lifetime acquire counter for *key*."""
        self._counts.pop(key, None)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        current = self._counts.get(key, 0)
        if current >= self._max_acquires:
            return False
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            self._counts[key] = current + 1
        return acquired

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
