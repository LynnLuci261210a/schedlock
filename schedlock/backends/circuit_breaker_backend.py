"""Circuit breaker wrapper backend for schedlock."""

import time
from schedlock.backends.base import BaseBackend


class CircuitBreakerBackend(BaseBackend):
    """Wraps a backend with a circuit breaker pattern.

    If the inner backend fails `failure_threshold` times within
    `window` seconds, the circuit opens and acquire returns False
    until `recovery_timeout` seconds have elapsed.
    """

    def __init__(self, inner: BaseBackend, failure_threshold: int = 3,
                 window: float = 60.0, recovery_timeout: float = 30.0):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if window <= 0:
            raise ValueError("window must be positive")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be positive")

        self._inner = inner
        self._failure_threshold = failure_threshold
        self._window = window
        self._recovery_timeout = recovery_timeout
        self._failures: list[float] = []
        self._opened_at: float | None = None

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def _prune_failures(self) -> None:
        cutoff = time.time() - self._window
        self._failures = [t for t in self._failures if t >= cutoff]

    def _is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if time.time() - self._opened_at >= self._recovery_timeout:
            self._opened_at = None
            self._failures = []
            return False
        return True

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if self._is_open():
            return False
        try:
            result = self._inner.acquire(key, owner, ttl)
            return result
        except Exception:
            self._failures.append(time.time())
            self._prune_failures()
            if len(self._failures) >= self._failure_threshold:
                self._opened_at = time.time()
            return False

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    @property
    def circuit_open(self) -> bool:
        return self._is_open()
