"""Token bucket rate-limiting backend wrapper."""
from __future__ import annotations

import time
from threading import Lock
from typing import Optional

from schedlock.backends.base import BaseBackend


class TokenBucketBackend(BaseBackend):
    """Wraps a backend and enforces a token-bucket rate limit on acquire calls.

    Tokens refill at *rate* tokens per *window* seconds up to *burst* tokens.
    If no token is available the acquire is denied immediately (no blocking).
    """

    def __init__(
        self,
        inner: BaseBackend,
        *,
        rate: float,
        window: float = 1.0,
        burst: Optional[float] = None,
    ) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if rate <= 0:
            raise ValueError("rate must be positive")
        if window <= 0:
            raise ValueError("window must be positive")
        burst = burst if burst is not None else rate
        if burst < 1:
            raise ValueError("burst must be >= 1")

        self._inner = inner
        self._rate = rate
        self._window = window
        self._burst = float(burst)
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = Lock()

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def rate(self) -> float:
        return self._rate

    @property
    def window(self) -> float:
        return self._window

    @property
    def burst(self) -> float:
        return self._burst

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * (self._rate / self._window)
        self._tokens = min(self._burst, self._tokens + new_tokens)
        self._last_refill = now

    def _consume(self) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if not self._consume():
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
