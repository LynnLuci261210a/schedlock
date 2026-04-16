"""TTL-enforcing backend wrapper that caps lock TTL to a configured maximum."""

from schedlock.backends.base import BaseBackend


class TTLCapBackend(BaseBackend):
    """Wraps a backend and enforces a maximum TTL on all acquire calls."""

    def __init__(self, inner: BaseBackend, max_ttl: int = 3600):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if max_ttl <= 0:
            raise ValueError("max_ttl must be a positive integer")
        self._inner = inner
        self._max_ttl = max_ttl

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        capped = min(ttl, self._max_ttl)
        return self._inner.acquire(key, owner, capped)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        capped = min(ttl, self._max_ttl)
        return self._inner.refresh(key, owner, capped)

    @property
    def max_ttl(self) -> int:
        return self._max_ttl
