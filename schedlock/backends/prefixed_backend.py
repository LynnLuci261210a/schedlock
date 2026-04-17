from schedlock.backends.base import BaseBackend


class PrefixedBackend(BaseBackend):
    """Wraps a backend and prepends a fixed prefix to all lock keys."""

    def __init__(self, inner: BaseBackend, prefix: str) -> None:
        if not isinstance(inner, BaseBackend):
            raise TypeError("inner must be a BaseBackend instance")
        if not prefix or not isinstance(prefix, str):
            raise ValueError("prefix must be a non-empty string")
        self._inner = inner
        self._prefix = prefix

    @property
    def inner(self) -> BaseBackend:
        return self._inner

    @property
    def prefix(self) -> str:
        return self._prefix

    def _prefixed(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    def acquire(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.acquire(self._prefixed(key), owner, ttl)

    def release(self, key: str, owner: str) -> bool:
        return self._inner.release(self._prefixed(key), owner)

    def is_locked(self, key: str) -> bool:
        return self._inner.is_locked(self._prefixed(key))

    def refresh(self, key: str, owner: str, ttl: int) -> bool:
        return self._inner.refresh(self._prefixed(key), owner, ttl)

    def get_owner(self, key: str):
        return self._inner.get_owner(self._prefixed(key))
