"""FencingBackend — wraps an inner backend and attaches a monotonically
increasing fencing token to every successful acquire.

Fencing tokens are used by callers to detect stale lock holders: a
higher token always wins.  The token is stored per lock-key and
incremented on every *successful* acquire.
"""
from __future__ import annotations

from typing import Optional

from schedlock.backends.base import BaseBackend


class FencingBackend(BaseBackend):
    """Wraps *inner* and stamps each successful acquire with a fencing token.

    Args:
        inner: The underlying backend to delegate to.

    Raises:
        TypeError: If *inner* is not a :class:`BaseBackend` instance.
    """

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._tokens: dict[str, int] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def token_for(self, key: str) -> int:
        """Return the current fencing token for *key* (0 if never acquired)."""
        return self._tokens.get(key, 0)

    def acquire(self, key: str, owner: str, ttl: int = 30) -> bool:
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            self._tokens[key] = self._tokens.get(key, 0) + 1
        return acquired

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int = 30) -> bool:
        return self._inner.refresh(key, owner, ttl)
