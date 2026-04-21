"""TimeBoundedBackend — wraps a backend and enforces a max lock hold duration.

If an owner tries to re-acquire or holds a lock beyond `max_hold_seconds`,
the backend forces a release and rejects the acquire.
"""

from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class TimeBoundedBackend(BaseBackend):
    """Wraps a backend and enforces a maximum hold duration per lock key.

    When a lock is acquired, the acquisition timestamp is recorded.  Any
    subsequent call to ``acquire`` or ``is_locked`` that finds the hold
    duration exceeded will automatically release the lock and report it as
    free, allowing a new owner to acquire it.
    """

    def __init__(self, inner: BaseBackend, max_hold_seconds: float) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(max_hold_seconds, (int, float)) or max_hold_seconds <= 0:
            raise ValueError("max_hold_seconds must be a positive number")
        self._inner = inner
        self._max_hold = float(max_hold_seconds)
        self._acquired_at: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def max_hold_seconds(self) -> float:
        return self._max_hold

    def _evict_if_overdue(self, key: str) -> None:
        """Release the lock if the max hold duration has been exceeded."""
        acquired_at = self._acquired_at.get(key)
        if acquired_at is not None and (time.monotonic() - acquired_at) > self._max_hold:
            self._inner.release(key, owner=self._get_current_owner(key))
            self._acquired_at.pop(key, None)

    def _get_current_owner(self, key: str) -> str:
        # Best-effort: attempt to read owner via is_locked; fall back to empty.
        try:
            locked, owner = self._inner.is_locked(key)
            return owner or ""
        except Exception:
            return ""

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        self._evict_if_overdue(key)
        result = self._inner.acquire(key, owner=owner, ttl=ttl)
        if result:
            self._acquired_at[key] = time.monotonic()
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner=owner)
        if result:
            self._acquired_at.pop(key, None)
        return result

    def is_locked(self, key: str) -> tuple[bool, Optional[str]]:
        self._evict_if_overdue(key)
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner=owner, ttl=ttl)
