"""DeadlineBackend — rejects acquire attempts after a wall-clock deadline."""
from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class DeadlineBackend(BaseBackend):
    """Wraps a backend and refuses to acquire locks after *deadline* (epoch seconds)."""

    def __init__(self, inner: BaseBackend, deadline: float) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(deadline, (int, float)):
            raise TypeError("deadline must be a numeric epoch timestamp")
        self._inner = inner
        self._deadline = float(deadline)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def deadline(self) -> float:
        return self._deadline

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if time.time() > self._deadline:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def __repr__(self) -> str:  # pragma: no cover
        return f"DeadlineBackend(deadline={self._deadline}, inner={self._inner!r})"
