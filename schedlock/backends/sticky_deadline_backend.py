"""StickyDeadlineBackend: rejects acquire attempts after a per-owner deadline has passed.

Once an owner first attempts to acquire a lock, a deadline is recorded.
Subsequent acquire attempts by that owner are rejected once the deadline
elapses, even if the inner backend would otherwise allow them.
"""

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class StickyDeadlineBackend(BaseBackend):
    """Wraps a backend and enforces a per-owner deadline window.

    Args:
        inner: The underlying backend to delegate to.
        deadline_seconds: Seconds from first-seen until the owner's deadline expires.
    """

    def __init__(self, inner: BaseBackend, deadline_seconds: float) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(deadline_seconds, (int, float)) or isinstance(deadline_seconds, bool):
            raise TypeError("deadline_seconds must be a number")
        if deadline_seconds <= 0:
            raise ValueError("deadline_seconds must be positive")

        self._inner = inner
        self._deadline_seconds = deadline_seconds
        # Maps owner -> first_seen_timestamp
        self._first_seen: dict[str, float] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def deadline_seconds(self) -> float:
        return self._deadline_seconds

    def _is_past_deadline(self, owner: str) -> bool:
        if owner not in self._first_seen:
            return False
        return (time.monotonic() - self._first_seen[owner]) > self._deadline_seconds

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        now = time.monotonic()
        if owner not in self._first_seen:
            self._first_seen[owner] = now

        if self._is_past_deadline(owner):
            return False

        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def reset_owner(self, owner: str) -> None:
        """Clear the recorded first-seen time for an owner, allowing them to acquire again."""
        self._first_seen.pop(owner, None)
