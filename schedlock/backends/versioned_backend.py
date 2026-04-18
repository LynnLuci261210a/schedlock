from schedlock.backends.base import BaseBackend


class VersionedBackend(BaseBackend):
    """Wraps a backend and tracks a monotonic version counter per lock key.

    Each successful acquire increments the version for that key.  The current
    version can be inspected via ``version_of``.  This is useful for detecting
    stale-lock scenarios or for optimistic-concurrency checks.
    """

    def __init__(self, inner: BaseBackend) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        self._inner = inner
        self._versions: dict[str, int] = {}

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    def version_of(self, key: str) -> int:
        """Return the current acquire-version for *key* (0 if never acquired)."""
        return self._versions.get(key, 0)

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        acquired = self._inner.acquire(key, owner, ttl)
        if acquired:
            self._versions[key] = self._versions.get(key, 0) + 1
        return acquired

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(key, owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(key)

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(key, owner, ttl)

    def reset_version(self, key: str) -> None:
        """Reset the version counter for *key* back to zero."""
        self._versions.pop(key, None)
