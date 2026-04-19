"""WindowBackend — only allows acquire within specified time-of-day windows."""

from __future__ import annotations

import datetime
from typing import Optional, List, Tuple

from schedlock.backends.base import BaseBackend


class WindowBackend(BaseBackend):
    """Wraps a backend and restricts acquire to allowed time-of-day windows.

    Each window is a (start, end) pair of datetime.time objects (UTC).
    Attempts outside all windows return False without touching the inner backend.
    """

    def __init__(
        self,
        inner: BaseBackend,
        windows: List[Tuple[datetime.time, datetime.time]],
    ) -> None:
        if inner is None:
            raise ValueError("inner backend is required")
        if not windows:
            raise ValueError("at least one time window is required")
        for start, end in windows:
            if not isinstance(start, datetime.time) or not isinstance(end, datetime.time):
                raise TypeError("window bounds must be datetime.time instances")
            if start >= end:
                raise ValueError(f"window start {start} must be before end {end}")
        self._inner = inner
        self._windows = list(windows)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def windows(self) -> List[Tuple[datetime.time, datetime.time]]:
        return list(self._windows)

    def _in_window(self, now: Optional[datetime.time] = None) -> bool:
        t = now or datetime.datetime.utcnow().time()
        return any(start <= t < end for start, end in self._windows)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if not self._in_window():
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
