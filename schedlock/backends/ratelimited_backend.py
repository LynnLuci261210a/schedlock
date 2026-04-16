"""A backend wrapper that enforces rate limiting on acquire calls."""
from schedlock.backends.base import BaseBackend
from schedlock.ratelimit import RateLimiter


class RateLimitedBackend(BaseBackend):
    """Wraps a backend and limits how often acquire may be called."""

    def __init__(self, inner: BaseBackend, limiter: RateLimiter):
        self._inner = inner
        self._limiter = limiter

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if not self._limiter.attempt():
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)
