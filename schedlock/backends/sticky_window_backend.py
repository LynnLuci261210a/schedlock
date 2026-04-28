"""StickyWindowBackend — only allows acquires within defined time windows,
and once acquired, sticks the owner until they release."""

from __future__ import annotations

import time
from typing import Callable, Optional

from schedlock.backends.base import BaseBackend


class StickyWindowBackend(BaseBackend):
    """Wraps a backend so that:
    - Acquires are only permitted when a schedule function returns True.
    - Once acquired, the owner is "sticky": no other owner can acquire
      until the current owner releases, even if the window closes.
    """

    def __init__(
        self,
        inner: BaseBackend,
        schedule_fn: Callable[[], bool],
        reason: str = "outside allowed window",
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(schedule_fn):
            raise TypeError("schedule_fn must be callable")
        if not reason or not isinstance(reason, str):
            raise ValueError("reason must be a non-empty string")
        self._inner = inner
        self._schedule_fn = schedule_fn
        self._reason = reason
        # key -> sticky owner
        self._sticky: dict[str, str] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def reason(self) -> str:
        return self._reason

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        # If a sticky owner holds this key, only they may re-acquire
        current_sticky = self._sticky.get(key)
        if current_sticky is not None and current_sticky != owner:
            return False
        # New acquire: window must be open
        if current_sticky is None and not self._schedule_fn():
            return False
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            self._sticky[key] = owner
        return acquired

    def release(self, key: str, owner: str) -> bool:
        released = self._inner.release(key, owner)
        if released:
            self._sticky.pop(key, None)
        return released

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
