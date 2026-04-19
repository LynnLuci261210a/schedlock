from schedlock.backends.base import BaseBackend


class BlacklistBackend(BaseBackend):
    """Wraps a backend and rejects acquire attempts from blacklisted owners."""

    def __init__(self, inner: BaseBackend, blacklisted: set):
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not blacklisted or not isinstance(blacklisted, (set, frozenset, list)):
            raise ValueError("blacklisted must be a non-empty collection")
        self._inner = inner
        self._blacklisted = frozenset(blacklisted)

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def blacklisted(self) -> frozenset:
        return self._blacklisted

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        if owner in self._blacklisted:
            return False
        return self._inner.acquire(key, owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        if owner in self._blacklisted:
            return False
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        if owner in self._blacklisted:
            return False
        return self._inner.refresh(key, owner, ttl)
