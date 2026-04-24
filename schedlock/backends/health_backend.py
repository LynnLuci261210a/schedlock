"""HealthBackend — wraps an inner backend and tracks liveness/health status."""
from __future__ import annotations

import time
from typing import Optional

from schedlock.backends.base import BaseBackend


class HealthBackend(BaseBackend):
    """Decorator backend that tracks health via consecutive failure counts.

    If *failure_threshold* consecutive acquire failures are observed the backend
    is marked *unhealthy* and all further acquire attempts are blocked until
    *recovery_window* seconds have elapsed without an additional failure.
    """

    def __init__(
        self,
        inner: BaseBackend,
        *,
        failure_threshold: int = 3,
        recovery_window: float = 60.0,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not isinstance(failure_threshold, int) or failure_threshold < 1:
            raise ValueError("failure_threshold must be a positive integer")
        if not isinstance(recovery_window, (int, float)) or recovery_window <= 0:
            raise ValueError("recovery_window must be a positive number")

        self._inner = inner
        self._failure_threshold = failure_threshold
        self._recovery_window = recovery_window
        self._consecutive_failures: int = 0
        self._unhealthy_since: Optional[float] = None

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def is_healthy(self) -> bool:
        if self._unhealthy_since is None:
            return True
        if time.monotonic() - self._unhealthy_since >= self._recovery_window:
            self._unhealthy_since = None
            self._consecutive_failures = 0
            return True
        return False

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if not self.is_healthy:
            return False
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._consecutive_failures = 0
        else:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._failure_threshold:
                self._unhealthy_since = time.monotonic()
        return result

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
