"""ScheduledBackend — only allows lock acquisition during defined schedule windows."""

from __future__ import annotations

import datetime
from typing import Callable, Optional

from schedlock.backends.base import BaseBackend


class ScheduledBackend(BaseBackend):
    """Wraps a backend and restricts acquires to a caller-supplied schedule function.

    The *schedule_fn* receives the current UTC datetime and returns ``True``
    when acquisitions are permitted.  Releases are always forwarded regardless
    of the schedule so held locks can be cleaned up.

    Args:
        inner: Underlying backend to delegate to.
        schedule_fn: Callable ``(datetime) -> bool`` that returns ``True`` when
            acquiring is allowed.
        reason: Human-readable label explaining the schedule restriction.
    """

    def __init__(
        self,
        inner: BaseBackend,
        schedule_fn: Callable[[datetime.datetime], bool],
        reason: str = "outside scheduled window",
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(schedule_fn):
            raise TypeError("schedule_fn must be callable")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")

        self._inner = inner
        self._schedule_fn = schedule_fn
        self._reason = reason

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def reason(self) -> str:
        return self._reason

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        now = datetime.datetime.utcnow()
        if not self._schedule_fn(now):
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
