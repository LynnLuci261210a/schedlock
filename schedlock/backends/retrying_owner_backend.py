"""RetryingOwnerBackend — retries acquire with a fresh owner on transient failures."""
from __future__ import annotations

import time
from typing import Callable, Optional

from schedlock.backends.base import BaseBackend


class RetryingOwnerBackend(BaseBackend):
    """Wraps an inner backend and retries acquire with a new owner on failure.

    Useful when the owner identity should be refreshed between attempts (e.g.
    short-lived tokens) or when you want bounded retry behaviour tied to owner
    rotation rather than a fixed owner string.

    Args:
        inner: The underlying backend to delegate to.
        owner_factory: Callable that produces a fresh owner string each call.
        retries: Number of additional attempts after the first (>= 0).
        delay: Seconds to wait between attempts (>= 0).
    """

    def __init__(
        self,
        inner: BaseBackend,
        owner_factory: Callable[[], str],
        retries: int = 2,
        delay: float = 0.1,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not callable(owner_factory):
            raise TypeError("owner_factory must be callable")
        if not isinstance(retries, int) or retries < 0:
            raise ValueError("retries must be a non-negative integer")
        if not isinstance(delay, (int, float)) or delay < 0:
            raise ValueError("delay must be a non-negative number")

        self._inner = inner
        self._owner_factory = owner_factory
        self._retries = retries
        self._delay = delay

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def retries(self) -> int:
        return self._retries

    @property
    def delay(self) -> float:
        return self._delay

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        # owner param is ignored; fresh owner produced by factory each attempt
        for attempt in range(self._retries + 1):
            fresh_owner = self._owner_factory()
            if self._inner.acquire(key, fresh_owner, ttl):
                return True
            if attempt < self._retries and self._delay > 0:
                time.sleep(self._delay)
        return False

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
