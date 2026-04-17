"""CachedBackend — wraps a backend and caches is_locked results for a short TTL."""

import time
from schedlock.backends.base import BaseBackend


class CachedBackend(BaseBackend):
    """Wraps an inner backend and caches is_locked() results to reduce load."""

    def __init__(self, inner: BaseBackend, cache_ttl: float = 1.0):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if cache_ttl <= 0:
            raise ValueError("cache_ttl must be positive")
        self._inner = inner
        self._cache_ttl = cache_ttl
        self._cache: dict[str, tuple[bool, float]] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        result = self._inner.acquire(key, owner, ttl)
        if result:
            self._cache[key] = (True, time.monotonic())
        return result

    def release(self, key: str, owner: str) -> bool:
        result = self._inner.release(key, owner)
        if result:
            self._cache[key] = (False, time.monotonic())
        return result

    def is_locked(self, key: str) -> bool:
        entry = self._cache.get(key)
        if entry is not None:
            value, ts = entry
            if time.monotonic() - ts < self._cache_ttl:
                return value
        result = self._inner.is_locked(key)
        self._cache[key] = (result, time.monotonic())
        return result

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def invalidate(self, key: str) -> None:
        """Manually invalidate the cache for a given key."""
        self._cache.pop(key, None)
