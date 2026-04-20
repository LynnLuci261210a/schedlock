"""WarmupBackend — delays lock acquisition until a warmup period has elapsed."""

from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class WarmupBackend(BaseBackend):
    """Blocks acquire calls until ``warmup_seconds`` have elapsed since
    construction.  Useful when a worker needs to warm up (e.g. load caches)
    before it is eligible to hold a distributed lock.

    Args:
        inner: The underlying backend to delegate to.
        warmup_seconds: How long (in seconds) to block acquisition.
    """

    def __init__(self, inner: BaseBackend, warmup_seconds: float) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(warmup_seconds, (int, float)) or warmup_seconds <= 0:
            raise ValueError("warmup_seconds must be a positive number")

        self._inner = inner
        self._warmup_seconds = float(warmup_seconds)
        self._started_at: float = time.monotonic()

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def warmup_seconds(self) -> float:
        return self._warmup_seconds

    def _is_warmed_up(self) -> bool:
        return (time.monotonic() - self._started_at) >= self._warmup_seconds

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if not self._is_warmed_up():
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
