"""LeakyBucketBackend — rate-limits acquire calls using a leaky bucket algorithm."""

import time
from collections import deque
from schedlock.backends.base import BaseBackend


class LeakyBucketBackend(BaseBackend):
    """Wraps a backend and enforces a leaky-bucket rate limit on acquire calls.

    Args:
        inner: The backend to delegate to.
        rate: Maximum number of acquires allowed per ``window`` seconds.
        window: Time window in seconds.
    """

    def __init__(self, inner: BaseBackend, rate: int = 5, window: float = 60.0) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if rate < 1:
            raise ValueError("rate must be at least 1")
        if window <= 0:
            raise ValueError("window must be positive")
        self._inner = inner
        self._rate = rate
        self._window = window
        self._timestamps: deque = deque()

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def rate(self) -> int:
        return self._rate

    @property
    def window(self) -> float:
        return self._window

    def _prune(self) -> None:
        cutoff = time.monotonic() - self._window
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        self._prune()
        if len(self._timestamps) >= self._rate:
            return False
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._timestamps.append(time.monotonic())
        return result

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
